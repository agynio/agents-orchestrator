# Ziti sidecar service loss investigation (2026-03-29)

## Environment

- Bootstrap cluster created via `/workspace/bootstrap/apply.sh -y`.
- KUBECONFIG: `/workspace/bootstrap/stacks/k8s/.kube/agyn-local-kubeconfig.yaml`.
- Gateway API accessed via `https://chat.agyn.dev:2496` with cluster admin token.
- Ziti controller `/version` reports v1.7.2 with `OIDC_AUTH` capabilities.

## Cluster status snapshot

```
platform-server-6dfd846d59-glvvd        0/1     CrashLoopBackOff   8 (50s ago)   18m
gateway-gateway-754d5f5b58-dqrcx        1/1     Running            1 (18m ago)   18m
llm-proxy-llm-proxy-c589f8bfb-vhvtz     1/1     Running            0             18m
ziti-management-556cd4b9d5-m925n        1/1     Running            0             18m
ziti-controller-7476c9fcc4-p5lw2        1/1     Running            0             22m
ziti-router-5cd86f97d9-sfnhq            1/1     Running            3 (21m ago)   21m
```

## Agent workloads created

```
workload-44f719e3-d204-43c6-830b-c176f3f2cb25   1/2     ImagePullBackOff   0          10m
workload-e54eb92b-4b25-4e55-bcd3-ffa8be5eee42   0/2     Error              0          5m3s
workload-572fbb2e-242b-49f5-9ecf-73a93355ace0   0/2     Error              0          105s
```

- `workload-44f719e3-...` uses `agent-image:latest` and fails with `ImagePullBackOff`.
- `workload-e54eb92b-...` uses `ghcr.io/agynio/agent-init-codex:0.6.0` as the main image and exits:
  `cp: can't stat '/agyn-bin/agynd/.': Not a directory`.
- `workload-572fbb2e-...` uses `debian:bookworm-slim` as the main image and exits with:
  `daemon init failed: get agent: rpc error: code = Unavailable desc = name resolver error: produced zero addresses`.

## Ziti sidecar service loss evidence

From `workload-572fbb2e-...` (`ziti-sidecar` init container):

```
time="2026-03-29T22:24:49.720Z" msg="adding service" service="gateway"
time="2026-03-29T22:24:49.722Z" msg="adding service" service="llm-proxy"
time="2026-03-29T22:24:50.722Z" msg="stopping tunnel for unavailable service: gateway"
time="2026-03-29T22:24:50.826Z" msg="stopping tunnel for unavailable service: llm-proxy"
```

Additional context:

- The sidecar enrolls successfully, adds intercept rules, then removes both services within ~1s.
- The sidecar exits with code 143 after the main container fails.

## Notes

- Gateway/LLM proxy logs show successful Ziti enrollment (`listening on ziti service gateway` / `llm-proxy`).
- Agents-orchestrator reconciler reports desired workloads increasing after agent creation.
