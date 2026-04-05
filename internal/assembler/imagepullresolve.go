package assembler

import (
	"context"
	"fmt"
	"sort"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
)

type imagePullResolver struct {
	secrets secretsv1.SecretsServiceClient
	cache   map[string]*runnerv1.ImagePullCredential
}

func newImagePullResolver(secrets secretsv1.SecretsServiceClient) *imagePullResolver {
	return &imagePullResolver{secrets: secrets, cache: map[string]*runnerv1.ImagePullCredential{}}
}

func (r *imagePullResolver) Resolve(ctx context.Context, attachments []*agentsv1.ImagePullSecretAttachment) error {
	for _, attachment := range attachments {
		if attachment == nil {
			return fmt.Errorf("image pull secret attachment is nil")
		}
		secretID := attachment.GetImagePullSecretId()
		if secretID == "" {
			return fmt.Errorf("image pull secret attachment %s image_pull_secret_id is empty", attachmentID(attachment))
		}
		if _, ok := r.cache[secretID]; ok {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := r.secrets.ResolveImagePullSecret(rctx, &secretsv1.ResolveImagePullSecretRequest{Id: secretID})
		cancel()
		if err != nil {
			return fmt.Errorf("resolve image pull secret %s: %w", secretID, err)
		}
		r.cache[secretID] = &runnerv1.ImagePullCredential{
			Registry: resp.GetRegistry(),
			Username: resp.GetUsername(),
			Password: resp.GetPassword(),
		}
	}
	return nil
}

func (r *imagePullResolver) Credentials() ([]*runnerv1.ImagePullCredential, error) {
	if len(r.cache) == 0 {
		return nil, nil
	}
	type imagePullEntry struct {
		id         string
		credential *runnerv1.ImagePullCredential
	}
	entries := make([]imagePullEntry, 0, len(r.cache))
	for id, credential := range r.cache {
		entries = append(entries, imagePullEntry{id: id, credential: credential})
	}
	sort.Slice(entries, func(i, j int) bool {
		leftRegistry := entries[i].credential.GetRegistry()
		rightRegistry := entries[j].credential.GetRegistry()
		if leftRegistry == rightRegistry {
			return entries[i].id < entries[j].id
		}
		return leftRegistry < rightRegistry
	})
	credentials := make([]*runnerv1.ImagePullCredential, 0, len(entries))
	registries := map[string]string{}
	for _, entry := range entries {
		registry := entry.credential.GetRegistry()
		if existing, ok := registries[registry]; ok && existing != entry.id {
			return nil, fmt.Errorf("registry conflict: registry %q is targeted by image pull secrets %s and %s", registry, existing, entry.id)
		}
		registries[registry] = entry.id
		credentials = append(credentials, entry.credential)
	}
	return credentials, nil
}

func attachmentID(attachment *agentsv1.ImagePullSecretAttachment) string {
	if attachment == nil {
		return ""
	}
	meta := attachment.GetMeta()
	if meta == nil {
		return ""
	}
	return meta.GetId()
}
