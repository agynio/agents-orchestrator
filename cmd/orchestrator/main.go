package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/db"
	"github.com/agynio/agents-orchestrator/internal/leader"
	"github.com/agynio/agents-orchestrator/internal/reconciler"
	"github.com/agynio/agents-orchestrator/internal/store"
	"github.com/agynio/agents-orchestrator/internal/subscriber"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/openziti/sdk-golang/ziti"
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

	var (
		runnerConn     *grpc.ClientConn
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
		zitiCtx, identityID, err := setupZitiIdentity(ctx, zitiMgmtClient)
		if err != nil {
			return err
		}
		go renewLease(ctx, zitiMgmtClient, identityID, cfg.ZitiLeaseRenewalInterval)
		runnerConn, err = grpc.NewClient(
			"passthrough:///runner",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return dialZitiWithRetry(ctx, zitiCtx, "runner")
			}),
		)
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
	} else {
		runnerConn, err = grpc.NewClient(cfg.RunnerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial runner: %w", err)
		}
	}
	defer closeConn(runnerConn)
	defer closeConn(zitiMgmtConn)

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	notificationsClient := notificationsv1.NewNotificationsServiceClient(notificationsConn)
	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	store := store.NewStore(pool)
	subscriber := subscriber.New(notificationsClient)
	assembler := assembler.New(agentsClient, secretsClient, &cfg)
	reconciler := reconciler.New(reconciler.Config{
		Threads:   threadsClient,
		Agents:    agentsClient,
		Runner:    runnerClient,
		ZitiMgmt:  zitiMgmtClient,
		Store:     store,
		Assembler: assembler,
		Wake:      subscriber.Wake(),
		Poll:      cfg.PollInterval,
		Idle:      cfg.IdleTimeout,
		StopSec:   cfg.StopTimeoutSec,
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

func setupZitiIdentity(ctx context.Context, client zitimgmtv1.ZitiManagementServiceClient) (ziti.Context, string, error) {
	identityResp, err := client.RequestServiceIdentity(ctx, &zitimgmtv1.RequestServiceIdentityRequest{
		ServiceType: zitimgmtv1.ServiceType_SERVICE_TYPE_ORCHESTRATOR,
	})
	if err != nil {
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

func dialZitiWithRetry(ctx context.Context, zitiCtx ziti.Context, service string) (net.Conn, error) {
	const (
		maxAttempts    = 5
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 10 * time.Second
	)
	backoff := initialBackoff
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := zitiCtx.DialContext(ctx, service)
		if err == nil {
			return conn, nil
		}
		log.Printf("dial ziti service %s: attempt %d/%d failed: %v", service, attempt, maxAttempts, err)
		lastErr = err
		if attempt == maxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return nil, fmt.Errorf("dial ziti service %s: %w", service, lastErr)
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
