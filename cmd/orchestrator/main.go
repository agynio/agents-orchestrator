package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	meteringv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/metering/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/leader"
	"github.com/agynio/agents-orchestrator/internal/reconciler"
	"github.com/agynio/agents-orchestrator/internal/runnerdial"
	"github.com/agynio/agents-orchestrator/internal/subscriber"
	"github.com/agynio/agents-orchestrator/internal/zitimanager"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("orchestrator: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	closeConn := func(name string, conn *grpc.ClientConn) {
		if conn == nil {
			return
		}
		if err := conn.Close(); err != nil {
			log.Printf("close %s connection: %v", name, err)
		}
	}

	threadsConn, err := grpc.NewClient(cfg.ThreadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial threads: %w", err)
	}
	defer closeConn("threads", threadsConn)

	notificationsConn, err := grpc.NewClient(cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer closeConn("notifications", notificationsConn)

	agentsConn, err := grpc.NewClient(cfg.AgentsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer closeConn("agents", agentsConn)

	secretsConn, err := grpc.NewClient(cfg.SecretsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial secrets: %w", err)
	}
	defer closeConn("secrets", secretsConn)

	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial runners: %w", err)
	}
	defer closeConn("runners", runnersConn)

	meteringConn, err := grpc.NewClient(cfg.MeteringServiceAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial metering: %w", err)
	}
	defer closeConn("metering", meteringConn)

	var (
		runnerDialer   runnerdial.RunnerDialer
		zitiMgmtConn   *grpc.ClientConn
		zitiMgmtClient zitimgmtv1.ZitiManagementServiceClient
	)
	// TODO: The E2E cluster does not yet deploy ziti-management or identities,
	// so we support a direct runner dial path for now. Remove this fallback
	// once ziti-management is part of the platform stack.
	if cfg.ZitiEnabled {
		zitiMgmtConn, err = grpc.NewClient(cfg.ZitiManagementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial ziti management: %w", err)
		}
		zitiMgmtClient = zitimgmtv1.NewZitiManagementServiceClient(zitiMgmtConn)
		manager, err := zitimanager.New(ctx, zitiMgmtClient, cfg.ZitiEnrollmentTimeout, cfg.ZitiLeaseRenewalInterval)
		if err != nil {
			return err
		}
		go manager.RunLeaseRenewal(ctx)
		runnerDialer = runnerdial.NewDialer(manager)
	} else {
		runnerConn, err := grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
		defer closeConn("runner", runnerConn)
		runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
		runnerDialer = runnerdial.NewFallbackDialer(runnerClient)
	}
	defer runnerDialer.Close()
	defer closeConn("ziti management", zitiMgmtConn)

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)
	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)
	meteringClient := meteringv1.NewMeteringServiceClient(meteringConn)
	subscriber := subscriber.New(notificationsClient, agentsClient, subscriber.WithServiceToken(cfg.InternalSubscribeToken))
	assembler := assembler.New(agentsClient, secretsClient, &cfg)
	reconciler := reconciler.New(reconciler.Config{
		Threads:                   threadsClient,
		Agents:                    agentsClient,
		RunnerDialer:              runnerDialer,
		ZitiMgmt:                  zitiMgmtClient,
		Runners:                   runnersClient,
		Metering:                  meteringClient,
		Assembler:                 assembler,
		Wake:                      subscriber.Wake(),
		Poll:                      cfg.PollInterval,
		WorkloadReconcileInterval: cfg.WorkloadReconcileInterval,
		Idle:                      cfg.IdleTimeout,
		StopSec:                   cfg.StopTimeoutSec,
		MeteringSampleInterval:    cfg.MeteringSampleInterval,
	})

	start := func(leadCtx context.Context) {
		group, groupCtx := errgroup.WithContext(leadCtx)
		group.Go(func() error {
			return subscriber.Run(groupCtx)
		})
		group.Go(func() error {
			return reconciler.Run(groupCtx)
		})
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("orchestrator: leader workload stopped: %v", err)
		}
	}

	leader, err := leader.New(&cfg, start)
	if err != nil {
		return err
	}

	log.Printf("orchestrator: ready")
	if err := leader.Run(ctx); err != nil {
		return err
	}
	return nil
}
