---
slug: modelserving-blog-post
title: An Analysis of the kthena's ModelServing
authors: [LiZhenCheng9527]
tags: []
---

# An Analysis of the kthena's ModelServing

## Introduction

As large models continue to grow exponentially in parameter size, the resource limits of a single virtual or physical machine can no longer meet their demands. To address this challenge, the industry has introduced innovative strategies such as PD-diaggregation deployment and hybrid deployment of large and small models. These approaches have transformed inference execution: instead of a single Pod handling an entire inference task, multiple Pods now often collaborate to complete a single prediction. This multi-Pod collaboration has become a key trend in large model inference deployment.

In practice, inference models may still run within a single Pod (as in traditional single-node scenarios), across a group of identical Pods (for larger models), or among Pods with specialized roles (as in PD-disaggregation deployments). This flexible deployment not only improves resource utilization but also enables more efficient large model inference.

`ModelServing` is a specialized component of `Kthena` designed to manage and orchestrate the lifecycle of inference model workloads. It can conveniently represent and manage multiple deployment models, such as `PD-disaggregation`, `tensor parallelism`, `pipeline parallelism`, and single-GPU model deployment, because its three-tier architecture.

## Three-tier Architecture

`ModelServing` adopts a three-tier architecture `ModelServing → servinggroups → roles` to address the limitations of Kubernetes' traditional two-tier architecture (e.g., deployment and statefulSet) in managing diverse inference workload deployment scenarios. The architecture diagram is shown below:

![ModelServing Architecture](images/modelServing_architecture.svg)

- **ModelServing:** The central component responsible for managing the lifecycle of inference model workloads. It offers a unified interface for deploying, monitoring, and managing inference models, as well as querying their statuses.
- **ServingGroups:** An `ServingGroup` is a collection of Roles, each fulfilling a specific function within the inference process, such as `Prefill` or `Decode`.
- **Roles:** Each Role within a `ServingGroup` consists of a group of Pods, which are the actual workloads responsible for executing inference tasks.

For the definition of `ModelServing`, please refer to the [modelServing crd reference](https://raw.githubusercontent.com/volcano-sh/kthena/refs/heads/main/docs/kthena/docs/reference/crd/workload.serving.volcano.sh.md).

After inference workloads are managed as groups, this approach aligns well with Volcano's gang scheduling and network topology-aware scheduling. However, basic capabilities required for traditional cloud-native workload management—such as rolling updates and scaling—still need to be handled additionally.

### Gang Scheduling

The Gang scheduling strategy is one of the core scheduling algorithms of the Volcano-Scheduler. It meets the scheduling requirements of “All or nothing” in the scheduling process and avoids the waste of cluster resources caused by arbitrary scheduling of Pod. The Gang scheduler algorithm is to observe whether the scheduled number of Pods meets the minimum number of runs. When the minimum number of runs of Job is satisfied, the scheduling action is executed for all Pods under Job; otherwise, it is not executed.

In Kthena, PodGroups are created based on ModelServing, leveraging Volcano's gang scheduling capabilities through PodGroups. The minTaskMember field specifies the number of pods in each Role that require gang scheduling.

#### Instance-Level Gang Scheduling

##### Creation Process

A single PodGroup is created for the entire ModelServing instance.

**PodGroup Naming Convention:**

- Name: `{modelserving-name}-instance`
- Labels and annotations include ModelServing metadata

**PodGroup Configuration:**

```yaml
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: {modelserving-name}-{servinggroup-index}
  namespace: {modelserving-namespace}
  labels:
    modelinfer.volcano.sh/name: {modelserving-name}-{servinggroup-index}
  annotations:
    scheduling.k8s.io/group-name: {modelserving-name}
spec:
  # MinTaskMember defines minimum pods required for each role replica
  minTaskMember:
    "prefill-0": 4    # 1 entry + 3 workers for prefill role replica 0
    "decode-0": 4     # 1 entry + 3 workers for decode role replica 0
    # ... additional role replicas
  minResources:
    # Aggregated resource requirements
  ttlSecondsAfterFinished: {timeout-seconds}
```

**Pod Count Calculation:**
If `MinRoleReplicas` is not configured, the value of `minTaskMember` is calculated as:

```math
minMember = replicas × Σ(role.replicas × (1 + role.workerReplicas))
```

Where:

- `replicas`: Number of ServingGroup instances
- `role.replicas`: Number of role instances within each ServingGroup
- `1 + role.workerReplicas`: EntryPod + WorkerPods per role instance

If `MinRoleReplicas` is configured, the value of `minMember` is calculated as:

**MinTaskMember Generation Logic:**

- For each role specified in MinRoleReplicas map
- Include role replicas from index 0 up to MinRoleReplicas[roleName] - 1
- Each role replica entry has value = (1 + role.workerReplicas)

**Example Configuration:**

```yaml
gangPolicy:
  minRoleReplicas:
    prefill: 2    # Require at least 2 prefill role replicas
    decode: 1     # Require at least 1 decode role replica
```

### Rolling Update

Rolling updates represent a critical operational strategy for online services aiming to achieve zero downtime. In the context of LLM inference services, the implementation of rolling updates is important to reduce the risk of service unavailability.

Currently, `ModelServing` supports rolling upgrades at the `ServingGroup` level, enabling users to configure `Partitions` to control the rolling process.

- Partition: Indicates the ordinal at which the `ModelServing` should be partitioned for updates. During a rolling update, replicas with an ordinal greater than or equal to `Partition` will be updated. Replicas with an ordinal less than `Partition` will not be updated.

Here's a ModelServing configured with rollout strategy:

```yaml
spec:
  rolloutStrategy:
    type: ServingGroupRollingUpdate
    rollingUpdateConfiguration:
      partition: 0
```

In the following we'll show how rolling update processes for a `ModelServing` with four replicas. Three Replica status are simulated here:

- ✅ Replica has been updated
- ❎ Replica hasn't been updated
- ⏳ Replica is in rolling update

|        | R-0 | R-1 | R-2 | R-3 | Note                                                                          |
|--------|-----|-----|-----|-----|-------------------------------------------------------------------------------|
| Stage1 | ✅   | ✅   | ✅   | ✅   | Before rolling update                                                         |
| Stage2 | ❎   | ❎   | ❎   | ⏳   | Rolling update started, The replica with the highest ordinal (R-3) is updated |
| Stage3 | ❎   | ❎   | ⏳   | ✅   | R-3 is updated. The next replica (R-2) is now being updated                   |
| Stage4 | ❎   | ⏳   | ✅   | ✅   | R-2 is updated. The next replica (R-1) is now being updated                   |
| Stage5 | ⏳   | ✅   | ✅   | ✅   | R-1 is updated. The last replica (R-0) is now being updated                   |
| Stage6 | ✅   | ✅   | ✅   | ✅   | Update completed. All replicas are on the new version                         |

During a rolling upgrade, the controller deletes and rebuilds the replica with the highest sequence number among the replicas need to be updated. The next replica will not be updated until the new replica is running normally.

### Scaling

In cloud-native infrastructure projects, scaling plays a crucial role in resource optimization and cost control, enhancing service availability and quickly response, and simplifying operations management.

In modelServing, as has two layers of resource descriptions, `ServingGroup` and `Role`. Therefore, we also support the scale up and scale down of the `ServingGroup level` and `role level`.

The handling of scaling and rolling updates at the servingGroup level is similar, with replicas being added or removed at the end of the queue.

Role-level scaling enables fine-grained adjustment of replica counts for each Role, such as in PD-separated deployment scenarios.

When the `role.Replicas` is modified, it triggers the Scaling of the role granularity.

When scaling is triggered, the status of the entire `ServingGroup` is set to scaling, and then the pod creation or deletion process is performed.

After the replicas of pods meets expectations. Then update the status of the ServingGroup based on the status of all the pods in the `ServingGroup`.

And because the pods in the role are carrying sequential and labeled. All scaling is processed from the last pod.

#### Role Scaling Process

Symbol meaning identical to [ServingGroup Scaling Process](#servinggroup-scaling-process)

|        | G-0 | G-1 | G-2 | G-3 | Note                                                                          |
|--------|-----|-----|-----|-----|-------------------------------------------------------------------------------|
| Stage1 | ✅   | ✅   | ✅   | ✅   | Before scaling up/down                                                         |
| Stage2 | ❎   | ❎   | ❎   | ⏳   | Scaling up/down started, The replica with the highest ordinal (G-3) is start. Creating/Deleting roles in the (G-3) |
| Stage3 | ❎   | ❎   | ⏳   | ✅   | G-3 is scaled. The roles in next replica (G-2) is now scaling                    |
| Stage4 | ❎   | ⏳   | ✅   | ✅   | G-2 is scaled. The roles in next replica (G-1) is now scaling                   |
| Stage5 | ⏳   | ✅   | ✅   | ✅   | G-1 is scaled. The roles in last replica (G-0) is now scaling                   |
| Stage6 | ✅   | ✅   | ✅   | ✅   | Scale completed.                         |

The overall processing flowchart is shown in the figure.

![Role Scaling Processing ](images/role_scaling.svg)

### Restart Policy

In `ModelServing`, the concept of grouping pods is emphasized. As a result, when an error occurs in one pod within a group, the entire group is typically restarted together. However, in production environments, restarting a whole group of pods can require significant resources and time. To address this, `ModelServing` also provides a restart policy that allows for individual pod restarts.

- **ServingGroupRecreate:** When an error occurs in pods within a group, the entire group is restarted.
- **RoleRecreate:** When an error occurs in pods within a group, the status of the group is updated to `progressing` and only the affected pod is restarted. If the `serviceGroup` remains in the `progressing` state for a certain period of time, the entire servingGroup will be deleted and recreated.

## Prospect

The current ModelServing is generally able to meet the basic requirements for model workload management and scheduling. However, support for PD-separated scenarios is still limited—for example, features such as PD instance upgrades are not yet fully supported. In future development, we plan to introduce more features specifically for PD-separated scenarios.

If you are interested, we welcome you to join the Kthena community and help build our open-source ecosystem together.
