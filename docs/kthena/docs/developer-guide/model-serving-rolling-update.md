# Rollout Strategy

Rolling updates represent a critical operational strategy for online services aiming to achieve zero downtime. In the context of LLM inference services, the implementation of rolling updates is important to reduce the risk of service unavailability.

Currently, `ModelServing` supports rolling upgrades at the `ServingGroup` level, enabling users to configure `Partitions` to control the rolling process.

- Partition: Protects the first N existing replicas in the datastore's ascending ordinal order. The remaining replicas are eligible for rolling update. With the normal contiguous ordinal set, this is equivalent to protecting ordinals in `[0, partition)`. Defining protection by list position also gives deterministic behavior for legacy or temporarily non-contiguous sets.

When a protected replica is missing and must be recreated, the ordinal itself selects the template: a missing ordinal below `partition` uses `CurrentRevision` and its historical template, while other missing ordinals use `UpdateRevision` and the current template. Before creating a `ServingGroup` that references a new revision, the controller must successfully persist its `ControllerRevision`; otherwise reconciliation stops and retries without creating a partial `ServingGroup`.

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

| | R-0 | R-1 | R-2 | R-3 | Note |
| --- | --- | --- | --- | --- | --- |
| Stage1 | ✅ | ✅ | ✅ | ✅ | Before rolling update |
| Stage2 | ❎ | ❎ | ❎ | ⏳ | Rolling update started; R-3 is selected first in this example |
| Stage3 | ❎ | ❎ | ⏳ | ✅ | R-3 is updated. The next replica (R-2) is now being updated |
| Stage4 | ❎ | ⏳ | ✅ | ✅ | R-2 is updated. The next replica (R-1) is now being updated |
| Stage5 | ⏳ | ✅ | ✅ | ✅ | R-1 is updated. The last replica (R-0) is now being updated |
| Stage6 | ✅ | ✅ | ✅ | ✅ | Update completed. All replicas are on the new version |

During a rolling upgrade, the controller selects an eligible outdated replica while respecting partition and availability constraints, then deletes and rebuilds it. Unhealthy outdated replicas are prioritized; ordinal order is used within the applicable candidate ordering. The controller does not proceed beyond the availability budget until replacement capacity is ready.
