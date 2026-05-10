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

## Full sidecar logs

From `agyn-workloads/workload-572fbb2e-242b-49f5-9ecf-73a93355ace0` (`ziti-sidecar` init container):

```
2026-03-29T22:24:46.747586065Z DEBUG: waiting 3s for /netfoundry/ziti_id.json (or token) to appear
2026-03-29T22:24:46.747634716Z INFO: identity file /netfoundry/ziti_id.json does not exist
2026-03-29T22:24:46.747638086Z INFO: looking for /var/run/secrets/netfoundry.io/enrollment-token/ziti_id.jwt
2026-03-29T22:24:46.747656657Z INFO: looking for /enrollment-token/ziti_id.jwt
2026-03-29T22:24:46.747659457Z INFO: looking for /netfoundry/ziti_id.jwt
2026-03-29T22:24:46.747661817Z INFO: enrolling /netfoundry/ziti_id.jwt
2026-03-29T22:24:46.806639760Z INFO    generating 4096 bit RSA key                  
2026-03-29T22:24:47.463896256Z INFO    enrolled successfully. identity file written to: /netfoundry/ziti_id.json 
2026-03-29T22:24:47.471204956Z DEBUG: evaluating positionals: tproxy
2026-03-29T22:24:47.471237827Z INFO: running "ziti tunnel tproxy --identity /netfoundry/ziti_id.json "
2026-03-29T22:24:47.519246214Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:94","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"udpIdleTimeout is less than 5s, using default value of 5m0s","time":"2026-03-29T22:24:47.518Z"}
2026-03-29T22:24:47.519274684Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:98","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"udpCheckInterval is less than 1s, using default value of 30s","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.519277954Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:101","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"tproxy config: lanIf            =  []","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.519280324Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:102","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"tproxy config: diverter         =  []","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.519282684Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:103","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"tproxy config: udpIdleTimeout   =  [5m0s]","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.519285065Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:104","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"tproxy config: udpCheckInterval =  [30s]","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.519301895Z {"file":"github.com/openziti/ziti/tunnel/intercept/iputils.go:51","func":"github.com/openziti/ziti/tunnel/intercept.SetDnsInterceptIpRange","level":"info","msg":"dns intercept IP range: 100.64.0.1 - 100.127.255.255","time":"2026-03-29T22:24:47.519Z"}
2026-03-29T22:24:47.527541574Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:283","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*interceptor).addIptablesChain","level":"info","msg":"added iptables 'mangle' link 'PREROUTING' --\u003e 'NF-INTERCEPT'","time":"2026-03-29T22:24:47.527Z"}
2026-03-29T22:24:47.527555105Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:144","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.New","level":"info","msg":"no lan interface specified with '-lanIf'. please ensure firewall accepts intercepted service addresses","time":"2026-03-29T22:24:47.527Z"}
2026-03-29T22:24:47.528183387Z {"error":"exec: \"resolvectl\": executable file not found in $PATH","file":"github.com/openziti/ziti/tunnel/dns/dns_linux.go:85","func":"github.com/openziti/ziti/tunnel/dns.flushDnsCaches","level":"warning","msg":"unable to find systemd-resolve or resolvectl in path, consider adding a dns flush to your restart process","time":"2026-03-29T22:24:47.528Z"}
2026-03-29T22:24:47.528187938Z {"file":"github.com/openziti/ziti/tunnel/dns/dns_linux.go:32","func":"github.com/openziti/ziti/tunnel/dns.NewDnsServer","level":"info","msg":"starting dns server...","time":"2026-03-29T22:24:47.528Z"}
2026-03-29T22:24:49.528404872Z {"file":"github.com/openziti/ziti/tunnel/dns/dns_linux.go:62","func":"github.com/openziti/ziti/tunnel/dns.NewDnsServer","level":"info","msg":"dns server running at 127.0.0.1:53","time":"2026-03-29T22:24:49.528Z"}
2026-03-29T22:24:49.528422822Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:205","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).AddHostname","level":"info","msg":"adding ziti-tunnel.resolver.test = 19.65.28.94 to resolver","time":"2026-03-29T22:24:49.528Z"}
2026-03-29T22:24:49.529103096Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:236","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).RemoveHostname","level":"info","msg":"removing ziti-tunnel.resolver.test from resolver","time":"2026-03-29T22:24:49.528Z"}
2026-03-29T22:24:49.529125716Z {"file":"github.com/openziti/ziti/tunnel/intercept/iputils.go:51","func":"github.com/openziti/ziti/tunnel/intercept.SetDnsInterceptIpRange","level":"info","msg":"dns intercept IP range: 100.64.0.1 - 100.127.255.255","time":"2026-03-29T22:24:49.529Z"}
2026-03-29T22:24:49.529129307Z {"file":"github.com/openziti/ziti/ziti/tunnel/root.go:181","func":"github.com/openziti/ziti/ziti/tunnel.startIdentity","level":"info","msg":"loading identity: /netfoundry/ziti_id.json","time":"2026-03-29T22:24:49.529Z"}
2026-03-29T22:24:49.720593804Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:155","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).HandleServicesChange","level":"info","msg":"adding service","service":"gateway","time":"2026-03-29T22:24:49.720Z"}
2026-03-29T22:24:49.720633345Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:226","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).addService","level":"info","msg":"checking if we can intercept newly available service gateway","serviceId":"2RSuTlxPDOv3qa9YNUpSC0","serviceName":"gateway","time":"2026-03-29T22:24:49.720Z"}
2026-03-29T22:24:49.720720637Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:234","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*interceptor).newTproxy","level":"info","msg":"tproxy listening on tcp:127.0.0.1:36665","time":"2026-03-29T22:24:49.720Z"}
2026-03-29T22:24:49.720780528Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:570","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).addInterceptAddr","level":"info","msg":"Adding rule iptables -t mangle -A NF-INTERCEPT [-m comment --comment gateway -d 100.64.0.2/32 -p tcp --dport 443:443 -j TPROXY --tproxy-mark 0x1/0x1 --on-ip=127.0.0.1 --on-port=36665]","time":"2026-03-29T22:24:49.720Z"}
2026-03-29T22:24:49.722984773Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:205","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).AddHostname","level":"info","msg":"adding gateway.ziti = 100.64.0.2 to resolver","time":"2026-03-29T22:24:49.722Z"}
2026-03-29T22:24:49.723025194Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:155","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).HandleServicesChange","level":"info","msg":"adding service","service":"llm-proxy","time":"2026-03-29T22:24:49.722Z"}
2026-03-29T22:24:49.723058805Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:226","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).addService","level":"info","msg":"checking if we can intercept newly available service llm-proxy","serviceId":"5SrOPaAirpK25GuW4N6alT","serviceName":"llm-proxy","time":"2026-03-29T22:24:49.723Z"}
2026-03-29T22:24:49.723167147Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:234","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*interceptor).newTproxy","level":"info","msg":"tproxy listening on tcp:127.0.0.1:35727","time":"2026-03-29T22:24:49.723Z"}
2026-03-29T22:24:49.723192728Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:570","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).addInterceptAddr","level":"info","msg":"Adding rule iptables -t mangle -A NF-INTERCEPT [-m comment --comment llm-proxy -d 100.64.0.3/32 -p tcp --dport 443:443 -j TPROXY --tproxy-mark 0x1/0x1 --on-ip=127.0.0.1 --on-port=35727]","time":"2026-03-29T22:24:49.723Z"}
2026-03-29T22:24:49.725259900Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:205","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).AddHostname","level":"info","msg":"adding llm-proxy.ziti = 100.64.0.3 to resolver","time":"2026-03-29T22:24:49.725Z"}
2026-03-29T22:24:50.722772514Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:278","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).removeService","level":"info","msg":"stopping tunnel for unavailable service: gateway","time":"2026-03-29T22:24:50.722Z"}
2026-03-29T22:24:50.722803294Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:311","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).acceptTCP","level":"error","msg":"error while accepting: accept tcp 127.0.0.1:36665: use of closed network connection","time":"2026-03-29T22:24:50.722Z"}
2026-03-29T22:24:50.722806514Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:314","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).acceptTCP","level":"info","msg":"shutting down","time":"2026-03-29T22:24:50.722Z"}
2026-03-29T22:24:50.722832695Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:639","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).StopIntercepting","level":"info","msg":"removing intercepted low-port: 443, high-port: 443","route":{"IP":"100.64.0.2","Mask":"/////w=="},"service":"gateway","time":"2026-03-29T22:24:50.722Z"}
2026-03-29T22:24:50.722874736Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:677","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).StopIntercepting","level":"info","msg":"Removing rule iptables -t mangle -A NF-INTERCEPT [-m comment --comment gateway -d 100.64.0.2/32 -p tcp --dport 443:443 -j TPROXY --tproxy-mark 0x1/0x1 --on-ip=127.0.0.1 --on-port=36665]","route":{"IP":"100.64.0.2","Mask":"/////w=="},"service":"gateway","time":"2026-03-29T22:24:50.722Z"}
2026-03-29T22:24:50.827022808Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:236","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).RemoveHostname","level":"info","msg":"removing gateway.ziti from resolver","time":"2026-03-29T22:24:50.826Z"}
2026-03-29T22:24:50.827043288Z {"file":"github.com/openziti/ziti/tunnel/intercept/svcpoll.go:278","func":"github.com/openziti/ziti/tunnel/intercept.(*ServiceListener).removeService","level":"info","msg":"stopping tunnel for unavailable service: llm-proxy","time":"2026-03-29T22:24:50.826Z"}
2026-03-29T22:24:50.827060409Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:311","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).acceptTCP","level":"error","msg":"error while accepting: accept tcp 127.0.0.1:35727: use of closed network connection","time":"2026-03-29T22:24:50.826Z"}
2026-03-29T22:24:50.827063089Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:314","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).acceptTCP","level":"info","msg":"shutting down","time":"2026-03-29T22:24:50.827Z"}
2026-03-29T22:24:50.827134900Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:639","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).StopIntercepting","level":"info","msg":"removing intercepted low-port: 443, high-port: 443","route":{"IP":"100.64.0.3","Mask":"/////w=="},"service":"llm-proxy","time":"2026-03-29T22:24:50.827Z"}
2026-03-29T22:24:50.827149240Z {"file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:677","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.(*tProxy).StopIntercepting","level":"info","msg":"Removing rule iptables -t mangle -A NF-INTERCEPT [-m comment --comment llm-proxy -d 100.64.0.3/32 -p tcp --dport 443:443 -j TPROXY --tproxy-mark 0x1/0x1 --on-ip=127.0.0.1 --on-port=35727]","route":{"IP":"100.64.0.3","Mask":"/////w=="},"service":"llm-proxy","time":"2026-03-29T22:24:50.827Z"}
2026-03-29T22:24:50.906937662Z {"file":"github.com/openziti/ziti/tunnel/dns/server.go:236","func":"github.com/openziti/ziti/tunnel/dns.(*resolver).RemoveHostname","level":"info","msg":"removing llm-proxy.ziti from resolver","time":"2026-03-29T22:24:50.906Z"}
2026-03-29T22:24:50.907182047Z {"chain":"NF-INTERCEPT","file":"github.com/openziti/ziti/tunnel/intercept/tproxy/tproxy_linux.go:415","func":"github.com/openziti/ziti/tunnel/intercept/tproxy.deleteIptablesChain","level":"info","msg":"removing iptables 'mangle' link 'PREROUTING' --\u003e 'NF-INTERCEPT'","time":"2026-03-29T22:24:50.907Z"}
2026-03-29T22:24:51.077387946Z {"error":"accept unix /tmp/gops-agent.39.sock: use of closed network connection","file":"github.com/openziti/agent@v1.0.32/agent.go:181","func":"github.com/openziti/agent.(*handler).listen","level":"error","msg":"error accepting gops connection, closing gops listener","time":"2026-03-29T22:24:51.077Z"}
2026-03-29T22:24:51.077409837Z {"error":"close unix /tmp/gops-agent.39.sock: use of closed network connection","file":"github.com/openziti/agent@v1.0.32/agent.go:176","func":"github.com/openziti/agent.(*handler).listen.func1","level":"error","msg":"error closing gops listener","time":"2026-03-29T22:24:51.077Z"}
```
