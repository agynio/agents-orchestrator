package zitimanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/openziti/edge-api/rest_model"
	"github.com/openziti/metrics"
	apis "github.com/openziti/sdk-golang/edge-apis"
	"github.com/openziti/sdk-golang/inspect"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewEnrollsIdentity(t *testing.T) {
	resetTestHooks(t)

	client := &fakeZitiMgmtClient{}
	client.requestServiceIdentity = func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
		return identityResponse("id-1"), nil
	}

	var gotConfig *ziti.Config
	ctx := &fakeZitiContext{}
	newZitiContext = func(cfg *ziti.Config) (ziti.Context, error) {
		gotConfig = cfg
		return ctx, nil
	}
	calledDisable := false
	disableOIDC = func(ziti.Context) error {
		calledDisable = true
		return nil
	}

	manager, err := New(client, 2*time.Second, time.Minute)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if manager.identityID != "id-1" {
		t.Fatalf("expected identity id id-1, got %q", manager.identityID)
	}
	if manager.zitiCtx != ctx {
		t.Fatal("expected ziti context to be stored")
	}
	if gotConfig == nil || gotConfig.ZtAPI != "https://example.test" {
		t.Fatalf("expected identity config to parse ztAPI, got %#v", gotConfig)
	}
	if !calledDisable {
		t.Fatal("expected OIDC to be disabled")
	}
}

func TestRunLeaseRenewalReEnrollsOnNotFound(t *testing.T) {
	resetTestHooks(t)
	leaseRetryBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	client := &fakeZitiMgmtClient{}
	var mu sync.Mutex
	requestCalls := 0
	reEnrollCh := make(chan struct{}, 1)
	client.requestServiceIdentity = func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requestCalls++
		if requestCalls > 1 {
			select {
			case reEnrollCh <- struct{}{}:
			default:
			}
		}
		return identityResponse(fmt.Sprintf("id-%d", requestCalls)), nil
	}
	client.extendIdentityLease = func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
		return nil, status.Error(codes.NotFound, "missing")
	}

	newZitiContext = func(*ziti.Config) (ziti.Context, error) {
		return &fakeZitiContext{}, nil
	}
	disableOIDC = func(ziti.Context) error { return nil }

	manager, err := New(client, time.Second, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		manager.RunLeaseRenewal(ctx)
	}()
	select {
	case <-reEnrollCh:
		cancel()
	case <-time.After(500 * time.Millisecond):
		cancel()
		t.Fatal("expected re-enrollment after lease NotFound")
	}
	waitForIdentity(t, manager, "id-2")
}

func TestNotifyAuthFailureReEnrolls(t *testing.T) {
	resetTestHooks(t)

	client := &fakeZitiMgmtClient{}
	requestCalls := 0
	client.requestServiceIdentity = func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
		requestCalls++
		return identityResponse(fmt.Sprintf("id-%d", requestCalls)), nil
	}

	newZitiContext = func(*ziti.Config) (ziti.Context, error) {
		return &fakeZitiContext{}, nil
	}
	disableOIDC = func(ziti.Context) error { return nil }

	manager, err := New(client, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	manager.NotifyAuthFailure()
	if manager.currentIdentityID() != "id-2" {
		t.Fatalf("expected re-enrolled identity id-2, got %q", manager.currentIdentityID())
	}
	if requestCalls != 2 {
		t.Fatalf("expected 2 enrollment calls, got %d", requestCalls)
	}
}

func TestNotifyAuthFailureDebounces(t *testing.T) {
	resetTestHooks(t)

	client := &fakeZitiMgmtClient{}
	requestCalls := 0
	reEnrollStart := make(chan struct{}, 1)
	releaseEnroll := make(chan struct{})
	client.requestServiceIdentity = func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
		requestCalls++
		if requestCalls == 1 {
			return identityResponse("id-1"), nil
		}
		select {
		case reEnrollStart <- struct{}{}:
		default:
		}
		<-releaseEnroll
		return identityResponse("id-2"), nil
	}

	newZitiContext = func(*ziti.Config) (ziti.Context, error) {
		return &fakeZitiContext{}, nil
	}
	disableOIDC = func(ziti.Context) error { return nil }

	manager, err := New(client, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.NotifyAuthFailure()
		}()
	}

	select {
	case <-reEnrollStart:
	case <-time.After(500 * time.Millisecond):
		close(releaseEnroll)
		wg.Wait()
		t.Fatal("expected re-enrollment to start")
	}
	close(releaseEnroll)
	wg.Wait()
	if requestCalls != 2 {
		t.Fatalf("expected 2 enrollment calls, got %d", requestCalls)
	}
}

func TestDialContextUsesCurrentContext(t *testing.T) {
	resetTestHooks(t)

	client := &fakeZitiMgmtClient{}
	requestCalls := 0
	client.requestServiceIdentity = func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
		requestCalls++
		return identityResponse(fmt.Sprintf("id-%d", requestCalls)), nil
	}

	dialedCh := make(chan string, 2)
	ctxCount := 0
	newZitiContext = func(*ziti.Config) (ziti.Context, error) {
		ctxCount++
		ctxID := fmt.Sprintf("ctx-%d", ctxCount)
		return &fakeZitiContext{
			dialContext: func(context.Context, string) (edge.Conn, error) {
				dialedCh <- ctxID
				return nil, nil
			},
		}, nil
	}
	disableOIDC = func(ziti.Context) error { return nil }

	manager, err := New(client, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := manager.DialContext(context.Background(), "service-a"); err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	first := <-dialedCh
	if first != "ctx-1" {
		t.Fatalf("expected dial with ctx-1, got %q", first)
	}

	manager.NotifyAuthFailure()
	if _, err := manager.DialContext(context.Background(), "service-b"); err != nil {
		t.Fatalf("DialContext after re-enroll: %v", err)
	}
	second := <-dialedCh
	if second != "ctx-2" {
		t.Fatalf("expected dial with ctx-2, got %q", second)
	}
}

func waitForIdentity(t *testing.T, manager *ZitiManager, expected string) {
	t.Helper()
	deadline := time.After(500 * time.Millisecond)
	for {
		if manager.currentIdentityID() == expected {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected identity %s, got %s", expected, manager.currentIdentityID())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func resetTestHooks(t *testing.T) {
	t.Helper()
	origNew := newZitiContext
	origDisable := disableOIDC
	origLease := leaseRetryBackoff
	t.Cleanup(func() {
		newZitiContext = origNew
		disableOIDC = origDisable
		leaseRetryBackoff = origLease
	})
}

func identityResponse(id string) *zitimgmtv1.RequestServiceIdentityResponse {
	return &zitimgmtv1.RequestServiceIdentityResponse{
		ZitiIdentityId: id,
		IdentityJson:   []byte(`{"ztAPI":"https://example.test"}`),
	}
}

type fakeZitiMgmtClient struct {
	createAgentIdentity    func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error)
	createAppIdentity      func(context.Context, *zitimgmtv1.CreateAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error)
	createService          func(context.Context, *zitimgmtv1.CreateServiceRequest, ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error)
	createRunnerIdentity   func(context.Context, *zitimgmtv1.CreateRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error)
	deleteAppIdentity      func(context.Context, *zitimgmtv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error)
	deleteIdentity         func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error)
	deleteRunnerIdentity   func(context.Context, *zitimgmtv1.DeleteRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error)
	listManagedIdentities  func(context.Context, *zitimgmtv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error)
	requestServiceIdentity func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error)
	extendIdentityLease    func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error)
}

func (f *fakeZitiMgmtClient) CreateAgentIdentity(ctx context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
	if f.createAgentIdentity != nil {
		return f.createAgentIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateAppIdentity(ctx context.Context, req *zitimgmtv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error) {
	if f.createAppIdentity != nil {
		return f.createAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateService(ctx context.Context, req *zitimgmtv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error) {
	if f.createService != nil {
		return f.createService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateRunnerIdentity(ctx context.Context, req *zitimgmtv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error) {
	if f.createRunnerIdentity != nil {
		return f.createRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteAppIdentity(ctx context.Context, req *zitimgmtv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error) {
	if f.deleteAppIdentity != nil {
		return f.deleteAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteIdentity(ctx context.Context, req *zitimgmtv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
	if f.deleteIdentity != nil {
		return f.deleteIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteRunnerIdentity(ctx context.Context, req *zitimgmtv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error) {
	if f.deleteRunnerIdentity != nil {
		return f.deleteRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListManagedIdentities(ctx context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
	if f.listManagedIdentities != nil {
		return f.listManagedIdentities(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ResolveIdentity(context.Context, *zitimgmtv1.ResolveIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.ResolveIdentityResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) RequestServiceIdentity(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
	if f.requestServiceIdentity != nil {
		return f.requestServiceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ExtendIdentityLease(ctx context.Context, req *zitimgmtv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
	if f.extendIdentityLease != nil {
		return f.extendIdentityLease(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeZitiContext struct {
	dialContext func(ctx context.Context, service string) (edge.Conn, error)
	closeFunc   func()
}

func (f *fakeZitiContext) Authenticate() error { return nil }

func (f *fakeZitiContext) GetExternalSigners() ([]*rest_model.ClientExternalJWTSignerDetail, error) {
	return nil, nil
}

func (f *fakeZitiContext) SetCredentials(apis.Credentials) {}

func (f *fakeZitiContext) LoginWithJWT(string) {}

func (f *fakeZitiContext) GetCredentials() apis.Credentials { return nil }

func (f *fakeZitiContext) GetCurrentIdentity() (*rest_model.IdentityDetail, error) { return nil, nil }

func (f *fakeZitiContext) GetCurrentIdentityWithBackoff() (*rest_model.IdentityDetail, error) {
	return nil, nil
}

func (f *fakeZitiContext) Dial(serviceName string) (edge.Conn, error) {
	return f.DialContext(context.Background(), serviceName)
}

func (f *fakeZitiContext) DialWithOptions(serviceName string, options *ziti.DialOptions) (edge.Conn, error) {
	return f.DialContext(context.Background(), serviceName)
}

func (f *fakeZitiContext) DialContext(ctx context.Context, serviceName string) (edge.Conn, error) {
	if f.dialContext != nil {
		return f.dialContext(ctx, serviceName)
	}
	return nil, nil
}

func (f *fakeZitiContext) DialContextWithOptions(ctx context.Context, serviceName string, options *ziti.DialOptions) (edge.Conn, error) {
	return f.DialContext(ctx, serviceName)
}

func (f *fakeZitiContext) DialAddr(network string, addr string) (edge.Conn, error) {
	return f.DialContext(context.Background(), addr)
}

func (f *fakeZitiContext) Listen(serviceName string) (edge.Listener, error) { return nil, nil }

func (f *fakeZitiContext) ListenWithOptions(serviceName string, options *ziti.ListenOptions) (edge.Listener, error) {
	return nil, nil
}

func (f *fakeZitiContext) GetServiceId(serviceName string) (string, bool, error) {
	return "", false, nil
}

func (f *fakeZitiContext) GetServices() ([]rest_model.ServiceDetail, error) { return nil, nil }

func (f *fakeZitiContext) GetService(serviceName string) (*rest_model.ServiceDetail, bool) {
	return nil, false
}

func (f *fakeZitiContext) GetServiceForAddr(network, hostname string, port uint16) (*rest_model.ServiceDetail, int, error) {
	return nil, 0, nil
}

func (f *fakeZitiContext) RefreshServices() error { return nil }

func (f *fakeZitiContext) RefreshService(serviceName string) (*rest_model.ServiceDetail, error) {
	return nil, nil
}

func (f *fakeZitiContext) GetServiceTerminators(serviceName string, offset, limit int) ([]*rest_model.TerminatorClientDetail, int, error) {
	return nil, 0, nil
}

func (f *fakeZitiContext) GetSession(id string) (*rest_model.SessionDetail, error) { return nil, nil }

func (f *fakeZitiContext) Metrics() metrics.Registry { return nil }

func (f *fakeZitiContext) Close() {
	if f.closeFunc != nil {
		f.closeFunc()
	}
}

func (f *fakeZitiContext) AddZitiMfaHandler(func(query *rest_model.AuthQueryDetail, resp ziti.MfaCodeResponse) error) {
}

func (f *fakeZitiContext) EnrollZitiMfa() (*rest_model.DetailMfa, error) { return nil, nil }

func (f *fakeZitiContext) VerifyZitiMfa(string) error { return nil }

func (f *fakeZitiContext) RemoveZitiMfa(string) error { return nil }

func (f *fakeZitiContext) GetId() string { return "" }

func (f *fakeZitiContext) SetId(string) {}

func (f *fakeZitiContext) Events() ziti.Eventer { return nil }

func (f *fakeZitiContext) Inspect() *inspect.ContextInspectResult { return nil }

var errNotImplemented = errors.New("not implemented")
