---
slug: release-v0.4.0-zh
title: "Kthena v0.4.0 正式发布：更稳定、功能更丰富的全新版本"
authors: [hzxuzhonghu, LiZhenCheng9527, YaoZengzeng]
tags: [release]
date: 2026-04-14
---

#  Kthena v0.4.0 正式发布：更稳定、功能更丰富的全新版本

感谢所有贡献者在过去两个月中的倾力付出与通力合作，带来了运行更加稳定，功能更加丰富的Kthena。我们对参与实现这一里程碑的每一个贡献者表示最诚挚的感谢。今天，我们很高兴地宣布 **Kthena v0.4.0** 正式发布，进一步简化大语言模型（LLM）工作负载的管理，为您的 AI 基础设施赋能。

<!-- truncate -->

## 更快、更智能的路由器 (Router)

### 确定且高效的模型选择

此前，由于 Kubernetes 内置的 CRD 校验无法强制实现跨对象的全局唯一性，将多个 `ModelRoute` 资源映射到同一个模型时可能会引发路由冲突，导致规则匹配出现歧义和目标选择的不一致。在Kthena v0.4.0,我们优雅地解决了这一问题。

Kthena v0.4.0 引入了可靠的[冲突解决机制](https://github.com/volcano-sh/kthena/pull/779)。当存在重复的 `ModelRoute` 时，路由器会确定性地优先选择最早创建（通常是预建）的路由，并将较新的重复项视为较低优先级。这确保了每次路由请求都能获得可预测且稳定的结果。

### 可配置的Prefix-Cache

统一的标准往往无法满足所有场景的需求。因此，Kthena 将硬编码的Prefix-Cache参数替换成了完全[可配置的Prefix-Cache系统](https://github.com/volcano-sh/kthena/pull/844)。您现在可以通过以下参数对Prefix-Cache的行为进行细粒度的控制：

- **Block Size（哈希处理块大小）：** 控制前缀匹配的粒度。较小的块能够提供更精确的匹配，但会增加 CPU 开销；而较大的块处理速度更快，但精准度会下降。
- **Max Block Limits（最大块限制）：** 设定了对给定提示词（prompt）进行哈希处理的上限。避免路由器在处理过长的传入提示词时，出现延迟激增。
- **Cache Capacity（缓存容量）：** 定义了路由器可以记住的前缀条目数量。增加此值可以提高多样化工作负载的缓存命中率，代价是略微增加内存占用。
- **Top-K Results（Top-K 结果数）：** 决定在匹配成功时考虑多少个候选实例。调整此参数有助于实现更好的负载均衡，确保流量平稳地分布在多个节点上，而不是让单个活跃实例过载。

通过微调这些设置，您可以定制 Kthena Router的Prefix-Cache，以更好地适配多样的模型和业务 LLM 工作负载。

## 细粒度、资源高效的滚动更新

过去，Kthena 能在整个 `ServingGroup` 级别执行滚动更新。但对于大规模的大型语言模型应用而言，完全重建一个 `ServingGroup` 往往是一个极其消耗资源且耗时的过程。

为了解决这个问题，我们引入了[基于 Role 的滚动更新机制](https://github.com/volcano-sh/kthena/pull/802)。当只有特定的 `Role` 需要变更时，您无需再更新整个 `ServingGroup`（这也是我们在恢复策略中引入 `RoleRecreate` 的原因）。从 v0.4.0 开始，可以动态调整 `rolloutStrategy`——这将大幅降低升级时的资源消耗，并显著缩短升级时ServingGroup的不可用时间。

## 支持SGLang和vLLM的PD分离部署

PD分离部署架构已经成为了大规模LLM服务的标准架构。在Kthena v0.4.0中，Kthena的`modelServing`和`Router`现已通过全面验证，能够良好地支持 **vLLM** 和 **SGLang**的PD分离部署。用户可以根据需求通过 `ModelServing` 配置 Prefill 和 Decode。并结合 `ModelServer` 中的 `pdGroup` 配置实现PD感知的智能路由，从而轻松构建高效的PD分离推理服务。

## 提升可观测性

### Role 状态可见性

为了最大限度地降低 kube-apiserver 的负载，Kthena 的 `ModelServing` 使用了本地存储缓存 `ServingGroup` 和 `Role` 的状态。虽然这种方式效率很高，但我们也意识到，这限制了Kthena的可观测性，使整个reconcile过程中`ServingGroup`和`Role`的状态变化变成了一个黑盒，无法观测。

在 v0.4.0 中，我们打破了这种“黑盒”状态。现在，我们能够[直接通过 Kubernetes Events 暴露 Role 的状态](https://github.com/volcano-sh/kthena/pull/676)，从而显著提升了 `ModelServing` 的可观测性。在未来的规划中，我们还计划根据用户需求将关键的 Role 信息直接嵌入到 `ModelServing` 的 Status 中，为您提供全面且透明的部署状态掌控能力。为开发者提供debug-port，可以直接拉取本地缓存中的`ServingGroup`和`Role`的状态。

### 全面的访问日志

现在，Router 会生成更详细的访问日志，为每一个请求捕获更丰富多样的[路由元数据 (routing metadata)](https://github.com/volcano-sh/kthena/pull/621)。以下是 Kthena v0.4.0 Router 日志的一个示例：

```sh
[2026-04-16T07:33:08.435627146Z] "POST /v1/chat/completions HTTP/1.1" 200 model_name=deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B model_router=deepseek-r1-1-1.5b model_server=deepseek-r1 selected_pod=deepseek-r1-1-1.5b-6989c66877-p6jvv request_id=ad683d1b-6011-4b0f-b9b5-cbb18d43c57b gateway=dev/default http_route=kthena-e2e-gie-8eoas/llm-route inference_pool=kthena-e2e-gie-8eoas/deepseek-r1-1-1.5b tokens=10/38 timings=3ms(0+2+0)
```

与之前的版本相比，我们新增了 `gateway`、`http_route` 和 `inference_pool` 字段，以便对Gateway及Gateway Inference Extension的流量提供丰富的信息。

## 开放的生态系统

我们致力于与广大的开源社区一道，将 Kthena 建设成为一个开放、包容且繁荣的项目。

### 支持 ModelScope (魔搭社区)

在 v0.4.0 中，我们在 Kthena 的模型下载器中扩展了对 [ModelScope 协议的支持](https://github.com/volcano-sh/kthena/pull/861)。这使得用户和运维管理人员可以更加灵活地选择符合其项目需求的模型仓库。

Kthena 拒绝绑定单一技术栈，而是坚持与多样化的 AI 技术无缝集成，从而不断深耕于充满活力的云原生 AI 生态之中。我们热忱地邀请全球开发者加入我们，共同构建这个包容且充满机遇的未来数字基础设施！
