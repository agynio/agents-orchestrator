package assembler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

type KubernetesSecretGetter struct {
	client corev1client.CoreV1Interface
}

func NewKubernetesSecretGetter(client corev1client.CoreV1Interface) KubernetesSecretGetter {
	if client == nil {
		panic("core v1 client is nil")
	}
	return KubernetesSecretGetter{client: client}
}

func (g KubernetesSecretGetter) Get(ctx context.Context, namespace string, name string) (*corev1.Secret, error) {
	return g.client.Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
}
