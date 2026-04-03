package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
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
	"github.com/openziti/sdk-golang/ziti"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	retryInitialBackoff = 1 * time.Second
	retryMaxBackoff     = 15 * time.Second
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

	closeConn := func(conn *grpc.ClientConn) {
		if conn == nil {
			return
		}
		_ = conn.Close()
	}

	threadsConn, err := grpc.DialContext(ctx, cfg.ThreadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial threads: %w", err)
	}
	defer closeConn(threadsConn)

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer closeConn(notificationsConn)

	agentsConn, err := grpc.DialContext(
		ctx,
		cfg.AgentsAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer closeConn(agentsConn)

	secretsConn, err := grpc.DialContext(ctx, cfg.SecretsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial secrets: %w", err)
	}
	defer closeConn(secretsConn)

	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial runners: %w", err)
	}
	defer closeConn(runnersConn)

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
		zitiCtx, identityID, err := setupZitiIdentity(ctx, zitiMgmtClient, cfg.ZitiEnrollmentTimeout)
		if err != nil {
			return err
		}
		go renewLease(ctx, zitiMgmtClient, identityID, cfg.ZitiLeaseRenewalInterval)
		runnerDialer = runnerdial.NewDialer(zitiCtx)
	} else {
		runnerConn, err := grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
		defer closeConn(runnerConn)
		runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
		runnerDialer = runnerdial.NewFallbackDialer(runnerClient)
	}
	if runnerDialer == nil {
		return fmt.Errorf("runner dialer not configured")
	}
	defer runnerDialer.Close()
	defer closeConn(zitiMgmtConn)

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)
	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)
	subscriber := subscriber.New(notificationsClient)
	assembler := assembler.New(agentsClient, secretsClient, &cfg)
	reconciler := reconciler.New(reconciler.Config{
		Threads:      threadsClient,
		Agents:       agentsClient,
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmtClient,
		Runners:      runnersClient,
		Assembler:    assembler,
		Wake:         subscriber.Wake(),
		Poll:         cfg.PollInterval,
		Idle:         cfg.IdleTimeout,
		StopSec:      cfg.StopTimeoutSec,
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

func setupZitiIdentity(ctx context.Context, client zitimgmtv1.ZitiManagementServiceClient, timeout time.Duration) (ziti.Context, string, error) {
	enrollmentCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var identityResp *zitimgmtv1.RequestServiceIdentityResponse
	if err := retryWithBackoff(enrollmentCtx, "ziti enrollment", func(attemptCtx context.Context) error {
		var requestErr error
		identityResp, requestErr = client.RequestServiceIdentity(attemptCtx, &zitimgmtv1.RequestServiceIdentityRequest{
			ServiceType: zitimgmtv1.ServiceType_SERVICE_TYPE_ORCHESTRATOR,
		})
		return requestErr
	}); err != nil {
		return nil, "", fmt.Errorf("request ziti service identity: %w", err)
	}
	identityID := identityResp.GetZitiIdentityId()
	if identityID == "" {
		return nil, "", fmt.Errorf("request ziti service identity: missing identity id")
	}
	identityJSON := identityResp.GetIdentityJson()
	if len(identityJSON) == 0 {
		return nil, "", fmt.Errorf("request ziti service identity: missing identity json")
	}
	identityConfig := &ziti.Config{}
	if err := json.Unmarshal(identityJSON, identityConfig); err != nil {
		return nil, "", fmt.Errorf("parse ziti identity: %w", err)
	}
	zitiCtx, err := ziti.NewContext(identityConfig)
	if err != nil {
		return nil, "", fmt.Errorf("load ziti identity: %w", err)
	}
	ctxImpl, ok := zitiCtx.(*ziti.ContextImpl)
	if !ok {
		return nil, "", fmt.Errorf("unexpected ziti context type %T; cannot disable OIDC", zitiCtx)
	}
	ctxImpl.CtrlClt.SetUseOidc(false)
	return zitiCtx, identityID, nil
}

func retryWithBackoff(ctx context.Context, operationName string, fn func(context.Context) error) error {
	backoff := retryInitialBackoff
	attempt := 1
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !isRetryableGrpcError(err) {
			return err
		}

		delay := backoff
		if delay > retryMaxBackoff {
			delay = retryMaxBackoff
		}

		log.Printf("%s failed (attempt %d), retrying in %s: %v", operationName, attempt, delay, err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
		attempt++
	}
}

func isRetryableGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	if !ok {
		return false
	}
	return statusErr.Code() == codes.Unavailable || statusErr.Code() == codes.Unknown
}

func renewLease(ctx context.Context, client zitimgmtv1.ZitiManagementServiceClient, identityID string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			if _, err := client.ExtendIdentityLease(ctx, &zitimgmtv1.ExtendIdentityLeaseRequest{ZitiIdentityId: identityID}); err != nil {
				log.Printf("failed to extend ziti lease: %v", err)
			}
		}
	}
}
