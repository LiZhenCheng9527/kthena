# Topology as an End-to-End Contract（llm-d × GKE）

## 场景

- **P/D 分离**：prefill 与 decode 是独立 Pod，每个请求的 KV cache 要通过 RDMA（RoCE）从 prefill GPU 传到 decode GPU。
- **传输路径由网络层决定，且默认互不协调**：
  - 平台：哪些网络路径存在
  - 资源分配器（DRA）：每个 worker 分到哪块 GPU 和 NIC
  - 路由器：哪个 prefill 与哪个 decode 配对
- **rail**：每块 GPU 有自己的 RoCE NIC，NIC 接入相互独立的交换平面。
  - 同 rail 传输只走一个平面。
  - 跨 rail 要上到更高层交换机换平面。
  - 每多一层交换机约 +2–3µs，而一次 KV 传输包含上千个内存描述符，延迟会直接累积到 TTFT。
- 测试平台：GCP A3 Ultra（H200）、A4（B200）、A4X（GB200 NVL72）、A4X Max（GB300 NVL72）。

## 发现的问题

核心结论：**问题的共同形态是"一层丢了另一层知道的信息，症状出现在远离根因的地方"。**

| # | 问题 | 现象 / 原因 |
|---|---|---|
| 1 | 链路存在 ≠ 路由可达 | Pod 网络缺少 per-NIC 路由和邻居表项，UCX 连接超时 |
| 2 | 平台能力差异 | A4X Max 裸金属不支持同节点 hairpin RDMA |
| 3 | GPU 与 NIC 未对齐 | A4X 系列 GPU 走 device plugin、NIC 走 DRA，可能落在不同 PCIe root |
| 4 | UCX 多网卡策略不当 | A3U/A4 一对 GPU 共享两块 NIC，大消息互相争用 |
| 5 | **静默回退 TCP** | RDMA 不可用时 UCX 不报错，服务层无故障信号，最后延迟升高再 OOM |
| 6 | **路由器不感知拓扑** | prefill/decode 配对不考虑 rail、host、rack，KV 路径更长、争用更多 |

## 三、解决方法：四个动作

1. **Discover**：平台把 PCIe root、NIC、rail 发布为 K8s 可见属性。
2. **Preserve**：用 DRA claim 加 `matchAttribute: resource.kubernetes.io/pcieRoot`，让 GPU 与 NIC 在同一 PCIe root。
   - A3U/A4 可完整表达；A4X 只能约束 NIC 侧。
   - UCX 调优：`UCX_SINGLE_NET_DEVICE=y` 做 GPU:NIC 1:1，A3U/A4 大消息带宽 +62%（依赖 UCX 1.22.1 / NIXL 1.4.1）。
3. **Verify**：让失败变得显式。
   - `UCX_TLS` 白名单去掉 TCP，回退时直接报 unreachable。
   - 排查：UCX 日志看是否为 `rc_mlx5`、对比 RDMA 计数器与 eth0 流量、`/proc/<pid>/maps` 看加载的 libibverbs。
   - 生产监控：per-NIC、秒级，包含 CNP、乱序、ACK 超时等拥塞与错误计数。
4. **Consume**：让路由器使用拓扑（下一节详述）。

---

## 四、重点：Router 如何根据拓扑选择 decode

### 4.1 为什么路由器的决定很关键
**prefill 与 decode 一旦配对，这个请求的 KV 传输路径就确定了。** 同 host、同 rack、跨 rack 的代价差别很大。分配与验证只保证"候选路径都可用"，选哪条由路由器决定。

### 4.2 llm-d v0.9 的实现

| 组件 | 作用 | PR |
|---|---|---|
| **topology-extractor** 插件 | 从每个 endpoint 的 Pod label 读取 host / rack / zone / region（label 键可配置） | llm-d-router#1678 |
| **topology-affinity filter** | 以第一阶段已选 endpoint 为基准，保留亲和度不低于下限的候选（默认到 host 级） | llm-d-router#2299 |
| **topology-affinity scorer** | 按与基准 endpoint 的邻近程度打分，host 优先 | llm-d-router#2299 |
| `stageOrder: prefill-first` | 在 disagg-profile-handler 中指定先选 prefill、再选 decode | llm-d-router#2342 |

### 4.3 推荐的选择流程（prefill 优先）

```
请求到达
  │
  ├─ 阶段 1：选 prefill
  │    依据：KV cache 复用（前缀命中）、负载
  │
  └─ 阶段 2：选 decode（decode profile 中配置 filter + scorer）
       ├─ filter：保留与"已选 prefill"拓扑亲和度 ≥ 下限的 decode（默认同 host）
       │          拓扑数据缺失，或没有候选满足 → fail-open，不过滤
       └─ scorer：按邻近程度打分（host → rack → zone → region），
                  与负载、缓存等其他 scorer 加权合成
```

**为什么 prefill 优先**：KV 复用是 prefill 的收益来源，先让它决定 prefill；
decode 只负责接收 KV，更适合在 prefill 确定后按"离谁近"来选。
文中说明两种顺序评分的都是同一对 pair 的邻近度，但 prefill 优先可以让 KV 复用决定 prefill。

### 4.4 两条设计原则

1. **可行性是硬约束，由路由器之下的层保证。**
   - 例如平台不支持同节点 hairpin，应由分配、放置或网络策略排除，不能依赖路由器 filter。
   - 因为路由器 filter 是 fail-open 的，它不是安全边界。
2. **成本是软评分。**
   - 在可行路径里偏好期望传输成本最低的配对。
   - 拓扑是主要信号但不是唯一信号；偏好域饱和时，应溢出到更远的 worker，而不是排队等"完美路径"。

### 4.5 拓扑信息从哪里来
- **路由器不直接访问云厂商硬件 API。** 由路由器之外的 DRA 插件、webhook 或 operator 发现物理布局并给 Pod 打 label。
- 原因：rail 网络、NVLink 域、TPU ICI slice 各自的拓扑形态不同，把它们抽象成统一的层级 label，路由器就可以保持通用。
- GKE 示例：节点带有 `cloud.google.com/gce-topology-block`、`-subblock`、`-host`；传播到 Pod 后可填充层级，例如把 sub-block 当作 rack。

### 4.6 实测：配对方式带来的收益

测试条件：2 节点 H200，gpt-oss-120b，8 个单 GPU prefill + 2 个 4 GPU decode，45 QPS，5400 请求（5000 输入、250 输出）。

| 配置 | 平均 TTFT | P90 |
|---|---|---|
| 无约束（对照） | 2.15s | 3.92s |
| DRA 对齐加 RDMA 验证，路由器默认调度 | 1.157s | 1.587s |
| 再加**拓扑评分配对** | **0.33s** | **0.41s** |

- 第二档到第三档，在同一部署、同一负载、同一已验证通道下，平均 TTFT 再降约 **3.5 倍**。
- 吞吐没有变化（43.8 vs 43.9 req/s，对比 45 QPS 的目标），说明收益不是靠少服务请求得到的。
- 25 次重复（跨主机和区域）：平均 TTFT 0.33s，范围 0.22–0.39s，标准差 0.04s。
- 原因：拓扑配对让路由器能控制的那部分传输走更短、争用更少的路径；负载下多个传输重叠，单次节省会累积。

---

## 五、其他实测收益

| 指标 | 结果 |
|---|---|
| 强制回退 TCP | 5400 请求中 436 个超时，平均延迟 223s（等同故障） |
| 验证过的 RDMA | 全部完成，单链路峰值约 359 Gbps（约 90% 线速） |
| A4X 消除 TCP 回退 | 约 48MB 的 KV 传输由 100ms 以上降到约 6.6ms，降幅超过 93% |
| A3U/A4 的 GPU:NIC 1:1 | 大消息带宽 +62% |

---

## 六、对 Kthena 的参考

### Kthena 现状

- **拓扑能力在 ModelServing 一侧**：`ModelServing.spec.template.networkTopology` 提供 `groupPolicy`（整个 ServingGroup）和 `rolePolicy`（单个 role 的实例），基于 Volcano 的 `NetworkTopologySpec`，**仅当调度器支持网络拓扑时生效**（`pkg/apis/workload/v1alpha1/servinggroup_types.go`）。
- **Router 侧 PD 配对**（`pkg/kthena-router/scheduler/scheduler_impl.go`）：
  1. 对 decode Pod 先跑 filter 和 score，取 TopN；
  2. 对每个候选 decode，只在**同一 PD group**（`PDGroup.GroupKey` 的 label 值相同）里取 prefill，再 filter、score，取最优；
  3. 得到 decode/prefill 候选对。
- 当前 Router 的 scorer 中**没有拓扑相关插件**（有 gpu、kvcache_aware、least_latency、least_request、lora_affinity、prefix_cache、random）。

## 其他

- **失败要显式**：为 PD 传输提供"是否真的走 RDMA"的检查（preflight、UCX 传输类型日志、NIC 计数器）。
- **指标**：per-NIC、秒级、含拥塞与错误计数；Router 可暴露"配对命中层级"（同 host / 同 rack / 跨 rack）的分布，用来观察拓扑评分是否起作用。
- **DRA 联合分配**：GPU 与 NIC 在同一 claim、同 PCIe root；视 Kthena 对 DRA 的支持规划而定。

---

## 参考
- 原文：https://llm-d.ai/blog/topology-as-an-end-to-end-contract
- 文中上游 PR：llm-d#1821、#2043、#2058、#2062、#2068；llm-d-router#1678、#2299、#2342
- Kthena：`pkg/kthena-router/scheduler/scheduler_impl.go`、`pkg/apis/workload/v1alpha1/servinggroup_types.go`、`docs/proposal/network-topology.md`
