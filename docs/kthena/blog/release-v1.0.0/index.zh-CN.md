---
slug: release-v1.0.0-zh-cn
title: "Kthena v1.0.0 正式发布：面向生产环境的 Kubernetes 原生大模型推理平台"
authors: [LiZhenCheng9527]
tags: [release]
date: 2026-06-30
---

## 概述

我们很高兴地宣布 **Kthena v1.0.0** 正式发布。这是 Kthena 在 Kubernetes 原生大模型推理领域迈出的重要一步。本次发布全面聚焦生产就绪能力：提升 Gateway API 路由准确性；为预填充/解码（P/D）分离工作负载提供原生的角色级自动扩缩容；实现更安全的角色级滚动更新；增强Router调度性能；能为多轮对话进行会话加速；通过 Prometheus 指标和示例仪表盘提供更丰富的缓存感知路由可观测性；并进一步完善 CLI 使用体验。

Kthena v1.0.0 还对autoscaler API 进行了重要整合。`AutoscalingPolicyBinding` 已被移除，目标配置现在通过 `homogeneousTarget`、`heterogeneousTarget` 和 `disaggregatedTarget` 直接定义在 `AutoscalingPolicy` 中。

<!-- truncate -->

## 版本亮点

### 核心特性概览

- **AutoscalingPolicy 整合与 P/D 协同式分离扩缩容：** 移除 `AutoscalingPolicyBinding`，将自动扩缩容目标配置统一整合到 `AutoscalingPolicy` 中；新增的 `disaggregatedTarget` 支持Perfill/Decode工作负载的角色级协同扩缩容。每个角色均可根据自身指标独立扩缩容，同时通过比例约束，将 P/D 副本比例维持在合理范围内。
- **面向多轮对话的会话加速：** Router可以优先处理近期已完成会话的后续请求，从而在高并发对话场景下提高 KV-Cache的命中率。
- **路由调度与可观测性增强：** 通过 Pod 级在途请求跟踪、基于 Redis 的多Router状态同步、可配置的 Pod 指标抓取，以及缓存感知 Prometheus 指标，提高调度准确性和运维可观测性。
- **角色级滚动更新可用性控制：** 进一步增强 `RoleRollingUpdate`，每个角色现在都可以通过 `maxUnavailable` 独立控制升级节奏。
- **Gateway API 与 HTTPRoute 行为修正：** Kthena Router现在能够遵循 HTTPRoute 主机名配置，在后端选择和 URL 重写过程中始终使用同一条已匹配的路由规则，并修正 `PathPrefix` 语义，同时遵循 Gateway 监听器的 `allowedRoutes` 配置。
- **CLI 与 OpenAI 兼容 API 增强：** CLI 提供更丰富的状态信息，并新增对 `ModelRoute` 和 `ModelServer` 的支持；Router新增兼容 OpenAI API 的 `GET /v1/models` 接口。

### AutoscalingPolicy 整合与 P/D 协同式分离扩缩容

Kthena v1.0.0 引入了一套更简洁、更强大的自动扩缩容 API。用户现在可以在单个 `AutoscalingPolicy` 资源中统一配置扩缩容目标、指标采集方式和扩缩容边界。

新的 `disaggregatedTarget` 模式专为基于Role的工作负载部署提供 **P/D 协同自动扩缩容**，尤其适用于PD分离部署。Prefill和Decode可以分别依据自身指标作出扩缩容决策，同时由自动扩缩容器应用共享约束，使两侧能够协调扩缩，而不会各自变化并逐渐偏离合理比例。每个角色都可以独立定义副本范围、指标和指标来源；运维人员还可以配置 `ratioConstraint`，将 P/D 副本比例维持在合理区间内。

`disaggregatedTarget` 配置示例：

```yaml
spec:
  disaggregatedTarget:
    targetRef:
      apiVersion: workload.serving.volcano.sh/v1alpha1
      kind: ModelServing
      name: vllm-qwen-pd-ms
    roles:
      prefill:
        minReplicas: 1
        maxReplicas: 8
        metrics:
          - name: prefill_waiting_requests
            targetValue: "1"
        metricSources:
          prefill_waiting_requests:
            prometheus:
              serverURL: http://kube-prometheus-stack-prometheus.test.svc.cluster.local:9090
              query: sum(vllm:num_requests_waiting{namespace="autoscale-demo", service="vllm-prefill"})
      decode:
        minReplicas: 1
        maxReplicas: 16
        metrics:
          - name: decode_gpu_cache_usage
            targetValue: "0.75"
        metricSources:
          decode_gpu_cache_usage:
            prometheus:
              serverURL: http://kube-prometheus-stack-prometheus.test.svc.cluster.local:9090
              query: sum(vllm:gpu_cache_usage_perc{namespace="autoscale-demo", service="vllm-decode"})
    ratioConstraint:
      numeratorRole: prefill
      denominatorRole: decode
      minRatio: "0.25"
      maxRatio: "1"
```

相关变更：

- 提案：
  - [Proposal of merge `autoscalingPolicybingding` into `autoscalingPolicy` #1172](https://github.com/volcano-sh/kthena/pull/1172)
- PR：
  - [merge autoscalingpolicybinding to autoscalingpolicy #1203](https://github.com/volcano-sh/kthena/pull/1203)
  - [Implementation of PD disaggregation auto-scaler #1258](https://github.com/volcano-sh/kthena/pull/1258)
- 贡献者：[@LiZhenCheng9527](https://github.com/LiZhenCheng9527)

### 面向多轮对话工作负载的会话加速

Kthena v1.0.0 新增会话加速能力，旨在优化多轮对话、智能体工作流和 RAG 链等后续请求依赖先前响应的场景。在这些工作负载中，后续请求通常会复用较长的公共前缀。如果请求在无关流量之后等待过久，对应后端中的 KV-Cache可能已被淘汰，进而增加TTFT。

会话加速允许Router跟踪近期完成的会话，并在等待队列中优先处理这些会话的后续请求。该机制与用户公平性调度相互独立，并提供专用的会话加速配置，包括会话请求头选择、等待请求准入上限，以及用于高级缓存命中优化的可选宽限期。

此功能旨在提高并发多轮请求下的缓存复用机会，但本身并不进行KV-Cache感知调度。若要最大限度发挥 KV-Cache优势，运维人员还应确保同时使用会话加速和KV-Cache感知调度。

Helm 配置示例：

```yaml
networking:
  kthenaRouter:
    sessionBoost:
      enabled: true
      header: X-Session-ID
      maxSessions: 4096
      inflightPerPod: 16
      gracePeriod: 0s
```

相关变更：

- Issue：[Improve multi-round conversation case #1190](https://github.com/volcano-sh/kthena/issues/1190)
- PR：
  - [session boost queue to optimize multi conversation scenario #1183](https://github.com/volcano-sh/kthena/pull/1183)
- 贡献者：[@YaoZengzeng](https://github.com/YaoZengzeng)、[@hzxuzhonghu](https://github.com/hzxuzhonghu)、[@FAUST-BENCHOU](https://github.com/FAUST-BENCHOU)、[@LiZhenCheng9527](https://github.com/LiZhenCheng9527)

### 更智能的路由调度与缓存感知可观测性

Router现在可以更好的利用的负载信号进行调度决策。Kthena 能够跟踪每个 Pod 的排队的请求数量，并通过 Redis 在多个Router副本之间同步这些计数器，使 `least-request` 插件能够依据实时全局负载作出决策，而不再局限于当前Router的本地状态。

缓存感知调度的可观测性也得到了显著增强。`prefix-cache` 和 `kvcache-aware` 评分插件现在通过Router现有的 `/metrics` 端点导出 Prometheus 指标，将以往仅记录在 klog 中的信息转化为可查询、带模型标签的时间序列，便于开展压力测试和进行请求调度。

为准确衡量缓存效果，Kthena 使用匹配比例直方图取代简单的命中/未命中计数器。`kthena_router_prefix_cache_match_ratio` 和 `kthena_router_kvcache_aware_match_ratio` 用于表示提示词中已存在于最佳匹配候选 Pod 上的数据块占比，其中 `0` 表示完全未命中。运维人员既可以通过 `le="0.0"` 桶推导缓存命中率，也可以直观了解Cache的实际复用程度。

相关变更：

- 提案：
  - [Observability for prefix-cache and kvcache-aware Score Plugins](https://github.com/volcano-sh/kthena/blob/main/docs/proposal/cache-observability.md)
- PR：
  - [feat(router): add per-pod in-flight request tracking with Redis sync #962](https://github.com/volcano-sh/kthena/pull/962)
  - [Add SGLang tokenizer support for KV-cache-aware scheduling #997](https://github.com/volcano-sh/kthena/pull/997)
  - [router: add observability metrics for prefix-cache and kvcache-aware score plugins #1194](https://github.com/volcano-sh/kthena/pull/1194)
  - [feat(router): make pod metrics update interval configurable #1151](https://github.com/volcano-sh/kthena/pull/1151)
  - [perf(router): cache parsed prompt to avoid redundant ParsePrompt call #1123](https://github.com/volcano-sh/kthena/pull/1123)
  - [fix: parallelize pod metrics scraping loop with bounded concurrency #1255](https://github.com/volcano-sh/kthena/pull/1255)
- 贡献者：[@hzxuzhonghu](https://github.com/hzxuzhonghu)、[@blenbot](https://github.com/blenbot)、[@kube-gopher](https://github.com/kube-gopher)、[@rajnish-jais](https://github.com/rajnish-jais)、[@nabrahma](https://github.com/nabrahma)

### 角色级滚动更新可用性控制

Kthena v0.4.0 引入了 `RoleRollingUpdate`，但在角色更新期间，系统会一次性删除 ServingGroup 中某个角色的全部旧副本。当 `spec.replicas` 为 `1` 时，这可能导致服务在角色级发布期间暂时不可用。

Kthena v1.0.0 为 `RoleRollingUpdate` 新增角色级 `maxUnavailable` 支持。运维人员现在可以使用绝对数量或百分比，为每个角色独立控制更新步长。角色级滚动更新由此具备与 ServingGroup 级更新类似的可用性控制能力。

角色级发布配置示例：

```yaml
spec:
  rolloutStrategy:
    type: RoleRollingUpdate
  template:
    roles:
    - name: prefill
      replicas: 2
      maxUnavailable: 1
      # entryTemplate and workerTemplate omitted
    - name: decode
      replicas: 4
      maxUnavailable: 25%
      # entryTemplate and workerTemplate omitted
```

相关变更：

- Issue：[Control the number of unavailable Role replicas in RoleRollingUpdate #1188](https://github.com/volcano-sh/kthena/issues/1188)
- PR：
  - [Role rollingupdate support maxUnavailable settings #1239](https://github.com/volcano-sh/kthena/pull/1239)
- 贡献者：[@hzxuzhonghu](https://github.com/hzxuzhonghu)、[@LiZhenCheng9527](https://github.com/LiZhenCheng9527)

### Gateway API 与 HTTPRoute 行为修正

Kthena Router现在能够更准确地处理 Gateway API 流量。Router会遵循 `HTTPRoute.spec.hostnames`，在后端选择和 URL 重写过滤器处理过程中始终使用同一条已匹配的 HTTPRoute 规则，并在同一路由内优先选择更具体的路径规则。由此可以避免请求误用其他链路中的后端。

本次发布还修正了 Gateway API 的 `PathPrefix` 匹配语义，并确保Router仅在满足 Gateway 监听器 `allowedRoutes` 约束时接纳 HTTPRoute。

相关变更：

- PR：
  - [feat: honor HTTPRoute hostnames and matched rule selection #1174](https://github.com/volcano-sh/kthena/pull/1174)
  - [Fix HTTPRoute PathPrefix matching #1119](https://github.com/volcano-sh/kthena/pull/1119)
  - [fix(router): respect Gateway allowedRoutes #1263](https://github.com/volcano-sh/kthena/pull/1263)
- 贡献者：[@zhy76](https://github.com/zhy76)、[@Monti-27](https://github.com/Monti-27)、[@avinxshKD](https://github.com/avinxshKD)

### CLI 与 OpenAI 兼容 API 增强

`kthena` CLI 现在能够展示更实用的状态信息，并支持更多资源类型：

- `kthena get model-servings` 新增 `READY` 和 `STATUS` 列。
- `kthena get model-boosters` 新增 `STATUS` 列。
- 新增对 `kthena get model-routes` 和 `kthena get model-servers` 的支持。
- 新增对 `kthena describe model-route` 和 `kthena describe model-server` 的支持。

Router还新增了兼容 OpenAI API 的 `GET /v1/models` 端点，以标准列表响应格式返回当前可用的模型名称。

相关变更：

- PR：
  - [feat: add STATUS and READY columns to kthena get output #978](https://github.com/volcano-sh/kthena/pull/978)
  - [feat: add CLI support for ModelRoute and ModelServer resources #981](https://github.com/volcano-sh/kthena/pull/981)
  - [feat: support /v1/models endpoint #996](https://github.com/volcano-sh/kthena/pull/996)
- 贡献者：[@anirudh240](https://github.com/anirudh240)、[@madmecodes](https://github.com/madmecodes)

## 其他功能增强

- 通过设置 scale 子资源标签选择器，为 `ModelServing` 新增 KEDA/HPA 兼容能力。[#839](https://github.com/volcano-sh/kthena/pull/839)
- 为 controller-manager 新增调试端口，可用于查看缓存中的 ServingGroup 和 Role 配置。[#900](https://github.com/volcano-sh/kthena/pull/900)
- 新增 SGLang Dynamo 模拟器测试覆盖与 SGLang 推理模拟器集成。[#920](https://github.com/volcano-sh/kthena/pull/920)、[#1231](https://github.com/volcano-sh/kthena/pull/1231)
- 为Router新增 `pprof` 端点支持。[#1057](https://github.com/volcano-sh/kthena/pull/1057)
- 在 Helm Chart 中新增 controller-manager 的 `debugPort` 配置。[#1032](https://github.com/volcano-sh/kthena/pull/1032)
- 改进 ModelBooster 的 GPU 与离线环境支持。[#972](https://github.com/volcano-sh/kthena/pull/972)、[#1141](https://github.com/volcano-sh/kthena/pull/1141)、[#1146](https://github.com/volcano-sh/kthena/pull/1146)、[#945](https://github.com/volcano-sh/kthena/pull/945)
- 新增 GPU 使用率插件 E2E 测试覆盖。[#1199](https://github.com/volcano-sh/kthena/pull/1199)
- 更新快速入门文档，并推荐用户优先从 ModelServing 开始使用 Kthena。[#1260](https://github.com/volcano-sh/kthena/pull/1260)
- 新增 DeepSeek-v4 模型服务示例。[#936](https://github.com/volcano-sh/kthena/pull/936)、[#937](https://github.com/volcano-sh/kthena/pull/937)
- 新增 KV 缓存感知调度器插件文档。[#910](https://github.com/volcano-sh/kthena/pull/910)

更加具体版本信息可以查看Kthena v1.0.0的Release Note：https://github.com/volcano-sh/kthena/releases/tag/v1.0.0

同时诚挚邀请广大开发者、运维人员和 AI 基础设施团队体验 Kthena v1.0.0，并与我们共同塑造下一代云原生大模型推理平台。
