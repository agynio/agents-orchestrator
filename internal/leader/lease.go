package leader

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/k8sclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type Leader struct {
	client    kubernetes.Interface
	namespace string
	name      string
	identity  string
	onStarted func(context.Context)
}

func New(cfg *config.Config, onStarted func(context.Context)) (*Leader, error) {
	if onStarted == nil {
		return nil, fmt.Errorf("onStarted must be provided")
	}
	identity := os.Getenv("HOSTNAME")
	if identity == "" {
		return nil, fmt.Errorf("HOSTNAME must be set")
	}
	namespace, err := k8sclient.ResolveNamespace(cfg.LeaseNamespace, "lease")
	if err != nil {
		return nil, err
	}
	name := cfg.LeaseName
	if name == "" {
		name = "agents-orchestrator"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return &Leader{
		client:    client,
		namespace: namespace,
		name:      name,
		identity:  identity,
		onStarted: onStarted,
	}, nil
}

func (l *Leader) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      l.name,
			Namespace: l.namespace,
		},
		Client: l.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: l.identity,
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: l.onStarted,
			OnStoppedLeading: func() {
				log.Printf("leader: lost leadership")
			},
		},
		ReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create leader elector: %w", err)
	}

	elector.Run(ctx)
	return nil
}
