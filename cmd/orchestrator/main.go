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
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/db"
	"github.com/agynio/agents-orchestrator/internal/leader"
	"github.com/agynio/agents-orchestrator/internal/reconciler"
	"github.com/agynio/agents-orchestrator/internal/runnerauth"
	"github.com/agynio/agents-orchestrator/internal/store"
	"github.com/agynio/agents-orchestrator/internal/subscriber"
	"github.com/jackc/pgx/v5/pgxpool"
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

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	threadsConn, err := grpc.DialContext(ctx, cfg.ThreadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial threads: %w", err)
	}
	defer threadsConn.Close()

	notificationsConn, err := grpc.DialContext(ctx, cfg.NotificationsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial notifications: %w", err)
	}
	defer notificationsConn.Close()

	agentsConn, err := grpc.DialContext(ctx, cfg.AgentsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial agents: %w", err)
	}
	defer agentsConn.Close()

	secretsConn, err := grpc.DialContext(ctx, cfg.SecretsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial secrets: %w", err)
	}
	defer secretsConn.Close()

	runnerUnaryInterceptor, runnerStreamInterceptor, err := runnerauth.NewClientInterceptors(cfg.RunnerSharedSecret)
	if err != nil {
		return fmt.Errorf("create runner auth interceptors: %w", err)
	}
	runnerConn, err := grpc.DialContext(ctx, cfg.RunnerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(runnerUnaryInterceptor),
		grpc.WithStreamInterceptor(runnerStreamInterceptor),
	)
	if err != nil {
		return fmt.Errorf("dial runner: %w", err)
	}
	defer runnerConn.Close()

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)
	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	store := store.NewStore(pool)
	subscriber := subscriber.New(notificationsClient)
	assembler := assembler.New(agentsClient, secretsClient, &cfg)
	reconciler := reconciler.New(threadsClient, agentsClient, runnerClient, store, assembler, subscriber.Wake(), cfg.PollInterval, cfg.IdleTimeout, cfg.StopTimeoutSec)

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

	if err := leader.Run(ctx); err != nil {
		return err
	}
	return nil
}
