package zitimanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	retryInitialBackoff = 1 * time.Second
	retryMaxBackoff     = 15 * time.Second
	leaseRetryBackoff   = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

	newZitiContext = ziti.NewContext
	disableOIDC    = disableZitiOIDC
)

type ZitiManager struct {
	mu              sync.RWMutex
	zitiCtx         ziti.Context
	identityID      string
	mgmtClient      zitimgmtv1.ZitiManagementServiceClient
	renewalInterval time.Duration
	enrollTimeout   time.Duration

	reEnrollMu         sync.Mutex
	reEnrollInProgress bool
}

func New(client zitimgmtv1.ZitiManagementServiceClient, enrollTimeout, renewalInterval time.Duration) (*ZitiManager, error) {
	if client == nil {
		return nil, errors.New("ziti management client missing")
	}
	if enrollTimeout <= 0 {
		return nil, fmt.Errorf("enroll timeout must be greater than 0")
	}
	if renewalInterval <= 0 {
		return nil, fmt.Errorf("lease renewal interval must be greater than 0")
	}
	manager := &ZitiManager{
		mgmtClient:      client,
		renewalInterval: renewalInterval,
		enrollTimeout:   enrollTimeout,
	}
	enrollCtx, cancel := context.WithTimeout(context.Background(), enrollTimeout)
	defer cancel()
	zitiCtx, identityID, err := manager.enroll(enrollCtx)
	if err != nil {
		return nil, err
	}
	manager.zitiCtx = zitiCtx
	manager.identityID = identityID
	return manager, nil
}

func (m *ZitiManager) DialContext(ctx context.Context, service string) (edge.Conn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.zitiCtx == nil {
		return nil, errors.New("ziti context missing")
	}
	return m.zitiCtx.DialContext(ctx, service)
}

func (m *ZitiManager) NotifyAuthFailure() {
	if err := m.triggerReEnroll(context.Background()); err != nil {
		log.Printf("ziti re-enroll after auth failure failed: %v", err)
	}
}

func (m *ZitiManager) RunLeaseRenewal(ctx context.Context) {
	ticker := time.NewTicker(m.renewalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			err := m.extendLeaseWithRetry(ctx)
			if err == nil {
				continue
			}
			if isNotFoundGrpcError(err) {
				if err := m.triggerReEnroll(ctx); err != nil {
					log.Printf("ziti lease renewal re-enroll failed: %v", err)
				}
				continue
			}
			if ctx.Err() == nil {
				log.Printf("failed to extend ziti lease: %v", err)
			}
		}
	}
}

func (m *ZitiManager) extendLeaseWithRetry(ctx context.Context) error {
	identityID := m.currentIdentityID()
	if identityID == "" {
		return errors.New("ziti identity id missing")
	}
	var lastErr error
	for attempt := 0; attempt <= len(leaseRetryBackoff); attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := m.mgmtClient.ExtendIdentityLease(ctx, &zitimgmtv1.ExtendIdentityLeaseRequest{ZitiIdentityId: identityID})
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == len(leaseRetryBackoff) {
			break
		}
		if waitErr := waitWithContext(ctx, leaseRetryBackoff[attempt]); waitErr != nil {
			return waitErr
		}
	}
	return lastErr
}

func (m *ZitiManager) currentIdentityID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityID
}

func (m *ZitiManager) triggerReEnroll(ctx context.Context) error {
	if !m.startReEnroll() {
		return nil
	}
	defer m.finishReEnroll()
	return m.reEnroll(ctx)
}

func (m *ZitiManager) startReEnroll() bool {
	m.reEnrollMu.Lock()
	defer m.reEnrollMu.Unlock()
	if m.reEnrollInProgress {
		return false
	}
	m.reEnrollInProgress = true
	return true
}

func (m *ZitiManager) finishReEnroll() {
	m.reEnrollMu.Lock()
	m.reEnrollInProgress = false
	m.reEnrollMu.Unlock()
}

func (m *ZitiManager) reEnroll(ctx context.Context) error {
	enrollCtx, cancel := context.WithTimeout(ctx, m.enrollTimeout)
	defer cancel()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.zitiCtx != nil {
		m.zitiCtx.Close()
		m.zitiCtx = nil
	}
	zitiCtx, identityID, err := m.enroll(enrollCtx)
	if err != nil {
		return err
	}
	m.zitiCtx = zitiCtx
	m.identityID = identityID
	return nil
}

func (m *ZitiManager) enroll(ctx context.Context) (ziti.Context, string, error) {
	var identityResp *zitimgmtv1.RequestServiceIdentityResponse
	if err := retryWithBackoff(ctx, "ziti enrollment", func(attemptCtx context.Context) error {
		var requestErr error
		identityResp, requestErr = m.mgmtClient.RequestServiceIdentity(attemptCtx, &zitimgmtv1.RequestServiceIdentityRequest{
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
	zitiCtx, err := newZitiContext(identityConfig)
	if err != nil {
		return nil, "", fmt.Errorf("load ziti identity: %w", err)
	}
	if err := disableOIDC(zitiCtx); err != nil {
		return nil, "", err
	}
	return zitiCtx, identityID, nil
}

func disableZitiOIDC(zitiCtx ziti.Context) error {
	ctxImpl, ok := zitiCtx.(*ziti.ContextImpl)
	if !ok {
		return fmt.Errorf("unexpected ziti context type %T; cannot disable OIDC", zitiCtx)
	}
	ctxImpl.CtrlClt.SetUseOidc(false)
	return nil
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

func isNotFoundGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	return ok && statusErr.Code() == codes.NotFound
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
