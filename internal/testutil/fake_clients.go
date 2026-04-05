package testutil

import (
	"context"
	"errors"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"google.golang.org/grpc"
)

var ErrNotImplemented = errors.New("not implemented")

type FakeAgentsClient struct {
	GetAgentFunc                       func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	ListSkillsFunc                     func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error)
	ListEnvsFunc                       func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	ListInitScriptsFunc                func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error)
	ListVolumeAttachmentsFunc          func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error)
	ListImagePullSecretAttachmentsFunc func(context.Context, *agentsv1.ListImagePullSecretAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error)
	ListMcpsFunc                       func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	ListHooksFunc                      func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error)
	GetVolumeFunc                      func(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
}

func (f *FakeAgentsClient) CreateAgent(context.Context, *agentsv1.CreateAgentRequest, ...grpc.CallOption) (*agentsv1.CreateAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateImagePullSecretAttachment(context.Context, *agentsv1.CreateImagePullSecretAttachmentRequest, ...grpc.CallOption) (*agentsv1.CreateImagePullSecretAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetImagePullSecretAttachment(context.Context, *agentsv1.GetImagePullSecretAttachmentRequest, ...grpc.CallOption) (*agentsv1.GetImagePullSecretAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteImagePullSecretAttachment(context.Context, *agentsv1.DeleteImagePullSecretAttachmentRequest, ...grpc.CallOption) (*agentsv1.DeleteImagePullSecretAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListImagePullSecretAttachments(ctx context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
	if f.ListImagePullSecretAttachmentsFunc != nil {
		return f.ListImagePullSecretAttachmentsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.GetAgentFunc != nil {
		return f.GetAgentFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateAgent(context.Context, *agentsv1.UpdateAgentRequest, ...grpc.CallOption) (*agentsv1.UpdateAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteAgent(context.Context, *agentsv1.DeleteAgentRequest, ...grpc.CallOption) (*agentsv1.DeleteAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListAgents(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateVolume(context.Context, *agentsv1.CreateVolumeRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	if f.GetVolumeFunc != nil {
		return f.GetVolumeFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateVolume(context.Context, *agentsv1.UpdateVolumeRequest, ...grpc.CallOption) (*agentsv1.UpdateVolumeResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteVolume(context.Context, *agentsv1.DeleteVolumeRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListVolumes(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateVolumeAttachment(context.Context, *agentsv1.CreateVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetVolumeAttachment(context.Context, *agentsv1.GetVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.GetVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteVolumeAttachment(context.Context, *agentsv1.DeleteVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
	if f.ListVolumeAttachmentsFunc != nil {
		return f.ListVolumeAttachmentsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateMcp(context.Context, *agentsv1.CreateMcpRequest, ...grpc.CallOption) (*agentsv1.CreateMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetMcp(context.Context, *agentsv1.GetMcpRequest, ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateMcp(context.Context, *agentsv1.UpdateMcpRequest, ...grpc.CallOption) (*agentsv1.UpdateMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteMcp(context.Context, *agentsv1.DeleteMcpRequest, ...grpc.CallOption) (*agentsv1.DeleteMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListMcps(ctx context.Context, req *agentsv1.ListMcpsRequest, opts ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
	if f.ListMcpsFunc != nil {
		return f.ListMcpsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateSkill(context.Context, *agentsv1.CreateSkillRequest, ...grpc.CallOption) (*agentsv1.CreateSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetSkill(context.Context, *agentsv1.GetSkillRequest, ...grpc.CallOption) (*agentsv1.GetSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateSkill(context.Context, *agentsv1.UpdateSkillRequest, ...grpc.CallOption) (*agentsv1.UpdateSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteSkill(context.Context, *agentsv1.DeleteSkillRequest, ...grpc.CallOption) (*agentsv1.DeleteSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListSkills(ctx context.Context, req *agentsv1.ListSkillsRequest, opts ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
	if f.ListSkillsFunc != nil {
		return f.ListSkillsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateHook(context.Context, *agentsv1.CreateHookRequest, ...grpc.CallOption) (*agentsv1.CreateHookResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetHook(context.Context, *agentsv1.GetHookRequest, ...grpc.CallOption) (*agentsv1.GetHookResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateHook(context.Context, *agentsv1.UpdateHookRequest, ...grpc.CallOption) (*agentsv1.UpdateHookResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteHook(context.Context, *agentsv1.DeleteHookRequest, ...grpc.CallOption) (*agentsv1.DeleteHookResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListHooks(ctx context.Context, req *agentsv1.ListHooksRequest, opts ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
	if f.ListHooksFunc != nil {
		return f.ListHooksFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateEnv(context.Context, *agentsv1.CreateEnvRequest, ...grpc.CallOption) (*agentsv1.CreateEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetEnv(context.Context, *agentsv1.GetEnvRequest, ...grpc.CallOption) (*agentsv1.GetEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateEnv(context.Context, *agentsv1.UpdateEnvRequest, ...grpc.CallOption) (*agentsv1.UpdateEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteEnv(context.Context, *agentsv1.DeleteEnvRequest, ...grpc.CallOption) (*agentsv1.DeleteEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest, opts ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
	if f.ListEnvsFunc != nil {
		return f.ListEnvsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateInitScript(context.Context, *agentsv1.CreateInitScriptRequest, ...grpc.CallOption) (*agentsv1.CreateInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetInitScript(context.Context, *agentsv1.GetInitScriptRequest, ...grpc.CallOption) (*agentsv1.GetInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateInitScript(context.Context, *agentsv1.UpdateInitScriptRequest, ...grpc.CallOption) (*agentsv1.UpdateInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteInitScript(context.Context, *agentsv1.DeleteInitScriptRequest, ...grpc.CallOption) (*agentsv1.DeleteInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListInitScripts(ctx context.Context, req *agentsv1.ListInitScriptsRequest, opts ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
	if f.ListInitScriptsFunc != nil {
		return f.ListInitScriptsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

type FakeSecretsClient struct {
	ResolveSecretFunc          func(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
	ResolveImagePullSecretFunc func(context.Context, *secretsv1.ResolveImagePullSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error)
}

func (f *FakeSecretsClient) CreateSecretProvider(context.Context, *secretsv1.CreateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.CreateSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) GetSecretProvider(context.Context, *secretsv1.GetSecretProviderRequest, ...grpc.CallOption) (*secretsv1.GetSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) UpdateSecretProvider(context.Context, *secretsv1.UpdateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) DeleteSecretProvider(context.Context, *secretsv1.DeleteSecretProviderRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ListSecretProviders(context.Context, *secretsv1.ListSecretProvidersRequest, ...grpc.CallOption) (*secretsv1.ListSecretProvidersResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) CreateSecret(context.Context, *secretsv1.CreateSecretRequest, ...grpc.CallOption) (*secretsv1.CreateSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) CreateImagePullSecret(context.Context, *secretsv1.CreateImagePullSecretRequest, ...grpc.CallOption) (*secretsv1.CreateImagePullSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) GetImagePullSecret(context.Context, *secretsv1.GetImagePullSecretRequest, ...grpc.CallOption) (*secretsv1.GetImagePullSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) UpdateImagePullSecret(context.Context, *secretsv1.UpdateImagePullSecretRequest, ...grpc.CallOption) (*secretsv1.UpdateImagePullSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) DeleteImagePullSecret(context.Context, *secretsv1.DeleteImagePullSecretRequest, ...grpc.CallOption) (*secretsv1.DeleteImagePullSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ListImagePullSecrets(context.Context, *secretsv1.ListImagePullSecretsRequest, ...grpc.CallOption) (*secretsv1.ListImagePullSecretsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ResolveImagePullSecret(ctx context.Context, req *secretsv1.ResolveImagePullSecretRequest, opts ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error) {
	if f.ResolveImagePullSecretFunc != nil {
		return f.ResolveImagePullSecretFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) GetSecret(context.Context, *secretsv1.GetSecretRequest, ...grpc.CallOption) (*secretsv1.GetSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) UpdateSecret(context.Context, *secretsv1.UpdateSecretRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) DeleteSecret(context.Context, *secretsv1.DeleteSecretRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ListSecrets(context.Context, *secretsv1.ListSecretsRequest, ...grpc.CallOption) (*secretsv1.ListSecretsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ResolveSecret(ctx context.Context, req *secretsv1.ResolveSecretRequest, opts ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
	if f.ResolveSecretFunc != nil {
		return f.ResolveSecretFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}
