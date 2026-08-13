---
sidebar_position: 3
---

# GPU-Free Quick Start

Evaluate Kthena end to end on a Kubernetes cluster **without GPUs or NPUs**.

This guide deploys a mock inference backend that mimics the vLLM API, exposes it
through Kthena's `ModelServer` and `ModelRoute` resources, and sends a test inference
request through the Kthena router. Everything runs on CPU-only nodes, so it works on a
laptop or in CI.

The steps below are platform-neutral. They were tested on a [Kind](https://kind.sigs.k8s.io/)
cluster, but the same workflow applies to Minikube, Docker Desktop Kubernetes, CI
clusters, and any other CPU-only Kubernetes environment.

## Prerequisites

- A running Kubernetes cluster (version 1.20 or later) with no GPU required.
  For a local cluster with Kind:

  ```bash
  kind create cluster --name kthena-demo
  ```

- `kubectl` configured to access the cluster
- `helm` (version 3.0 or later)
- Kthena installed on the cluster — see [Installation](./installation.md). The quickest
  path:

  ```bash
  helm install kthena oci://ghcr.io/volcano-sh/charts/kthena --version v1.0.0 --namespace kthena-system --create-namespace
  ```

- [Volcano](https://volcano.sh/en/docs/installation/) installed if you plan to explore
  `ModelServing` workloads afterwards. It is **not required** for this guide: the mock
  backend is a plain Kubernetes `Deployment` scheduled by the default scheduler.

## Step 1: Verify Kthena Components and CRDs

Check that the control plane components are running:

```bash
kubectl get pods -n kthena-system
```

Expected output (names will vary):

```text
NAME                                        READY   STATUS    RESTARTS   AGE
kthena-controller-manager-xxxxxxxxx-xxxxx   1/1     Running   0          1m
kthena-router-xxxxxxxxx-xxxxx               1/1     Running   0          1m
```

Check that the Kthena CRDs are installed:

```bash
kubectl get crd | grep volcano.sh
```

Expected output includes the networking and workload CRDs (list from Kthena v1.0.0;
newer versions may install additional CRDs):

```text
autoscalingpolicies.workload.serving.volcano.sh
modelboosters.workload.serving.volcano.sh
modelroutes.networking.serving.volcano.sh
modelservers.networking.serving.volcano.sh
modelservings.workload.serving.volcano.sh
```

## Step 2: Deploy the Mock Inference Backend

The repository ships a mock backend that emulates a vLLM server: it exposes the same
OpenAI-compatible HTTP API and metrics endpoint, but returns simulated responses
instead of running a real model — no GPU, no model weights.

```bash
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/LLM-Mock-ds1.5b.yaml
```

Wait for the mock Pods to become ready:

```bash
kubectl wait --for=condition=Ready pod -l app=deepseek-r1-1-5b --timeout=180s
kubectl get pods -l app=deepseek-r1-1-5b
```

Expected output:

```text
NAME                                READY   STATUS    RESTARTS   AGE
deepseek-r1-1-5b-xxxxxxxxx-xxxxx    1/1     Running   0          1m
deepseek-r1-1-5b-xxxxxxxxx-xxxxx    1/1     Running   0          1m
deepseek-r1-1-5b-xxxxxxxxx-xxxxx    1/1     Running   0          1m
```

## Step 3: Create the ModelServer and ModelRoute

A `ModelServer` tells the router which Pods serve a model (via a workload selector and
port). A `ModelRoute` declares a routable model name and maps it to one or more
`ModelServer` targets.

```bash
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/ModelServer-ds1.5b.yaml
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/ModelRouteSimple.yaml
```

Verify both resources exist:

```bash
kubectl get modelservers,modelroutes
```

Expected output:

```text
NAME                                                              AGE
modelserver.networking.serving.volcano.sh/deepseek-r1-1-5b        1m

NAME                                                              AGE
modelroute.networking.serving.volcano.sh/deepseek-simple          1m
```

## Step 4: Port-Forward the Kthena Router

The router Service is of type `LoadBalancer`. On clusters without a load-balancer
implementation (such as a default Kind cluster) its external IP stays `<pending>`, so
use a port-forward for local access:

```bash
kubectl port-forward -n kthena-system svc/kthena-router 8080:80
```

Keep this command running and continue in a second terminal.

## Step 5: Send a Test Inference Request

The `ModelRoute` above registers the model name `deepseek-simple`. Send an
OpenAI-style completion request through the router:

```bash
curl -N http://localhost:8080/v1/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "deepseek-simple",
        "prompt": "San Francisco is a",
        "max_tokens": 5,
        "temperature": 0,
        "stream": true
    }'
```

Two request fields matter with the current mock image: `max_tokens` is required (the
mocker needs an explicit output length), and `stream: true` avoids a router/mock
interaction documented in the troubleshooting section below.

Expected response is a stream of SSE chunks with simulated tokens, ending in `[DONE]`:

```text
data: {"id":"cmpl-4282ab27-171f-4ccf-82a3-adbebc84151a","choices":[{"text":"En","index":0}],"created":1786508011,"model":"deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B","system_fingerprint":null,"object":"text_completion","usage":null}

...

data: {"id":"cmpl-4282ab27-171f-4ccf-82a3-adbebc84151a","choices":[{"text":"","index":0,"finish_reason":"length"}],"created":1786508011,"model":"deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B","system_fingerprint":null,"object":"text_completion","usage":null,"nvext":{"timing":{"request_received_ms":1786508011068,"total_time_ms":54.605582999999996}}}

data: [DONE]
```

Note that the response reports the backend model name
(`deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B`) even though the request used the routed
name `deepseek-simple`: the router matched the `model` field against the `ModelRoute`,
rewrote it to the `ModelServer`'s base model, picked a ready backend Pod from the
`ModelServer`'s selector, and forwarded the request.

## Step 6: Troubleshooting and Resource Inspection

**Non-streaming request fails with `"request to all pods failed"` (HTTP 404).** The
router log (`kubectl logs -n kthena-system deploy/kthena-router`) shows
`http resp error, http code is 400` for every backend attempt. For non-streaming
requests the router adds an `include_usage` field to the forwarded request body, and
strict OpenAI-compatible backends — including current builds of the mock image — reject
unknown parameters (`Validation: Unsupported parameter(s): include_usage`). Use
`"stream": true` as shown above. Requests without `max_tokens` fail similarly, with the
mock returning `max_output_tokens must be specified for mocker`.

**Request returns an error or no route is matched.** Confirm the `model` field in the
request matches `spec.modelName` of the `ModelRoute`, then inspect the resources:

```bash
kubectl describe modelroute deepseek-simple
kubectl describe modelserver deepseek-r1-1-5b
```

**Router does not pick up backend Pods.** The `ModelServer`'s `workloadSelector` must
match the Pod labels, and the Pods must be `Ready`:

```bash
kubectl get pods -l app=deepseek-r1-1-5b -o wide
```

**Check the router logs.** Routing decisions and configuration errors are visible in
the router log:

```bash
kubectl logs -n kthena-system deploy/kthena-router
```

**Mock Pods stay in `ContainerCreating`.** The mock backend image is hosted on
`ghcr.io`, and on a slow connection the pull can take a long time — check with
`kubectl describe pod <pod-name>` (a single `Pulling image ...` event with no error
means the pull is still in progress). To bypass a slow or blocked in-cluster pull,
pull the image locally and load it into the cluster (Kind example):

```bash
docker pull ghcr.io/faust-benchou/dynamo-mocker-vllm:latest
kind load docker-image ghcr.io/faust-benchou/dynamo-mocker-vllm:latest --name kthena-demo
```

## Step 7: Cleanup

Remove the demo resources:

```bash
kubectl delete -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/ModelRouteSimple.yaml
kubectl delete -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/ModelServer-ds1.5b.yaml
kubectl delete -f https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/examples/kthena-router/LLM-Mock-ds1.5b.yaml
```

Expected output (older kubectl versions omit the `from default namespace` suffix):

```text
modelroute.networking.serving.volcano.sh "deepseek-simple" deleted from default namespace
modelserver.networking.serving.volcano.sh "deepseek-r1-1-5b" deleted from default namespace
deployment.apps "deepseek-r1-1-5b" deleted from default namespace
```

To remove Kthena and the local cluster entirely:

```bash
helm uninstall kthena -n kthena-system
kind delete cluster --name kthena-demo
```

## Limitations of Mock Inference

The mock backend is for evaluating Kthena's control plane and routing behavior, not
model quality or performance:

- Responses are canned strings, not real model output; token counts and timings in the
  response are simulated.
- Latency, throughput, and KV-cache utilization do not reflect a real inference engine,
  so load-aware and KV-cache-aware scheduling behave on synthetic metrics only.
- Engine-specific features such as LoRA adapter loading are only emulated at the API
  level.

## Next Steps

- Deploy a real model with GPUs following the [Quick Start](./quick-start.md)
- Explore routing strategies in [Router Routing](../user-guide/router-routing.md)
- Expose the router through the Gateway API — see
  [Gateway API Support](../user-guide/gateway-api-support.md)
