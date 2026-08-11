---
title: Support maxSurge for ModelServing
authors:
- "@LiZhenCheng9527"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-08-11

---

## Support maxSurge for ModelServing

### Summary

This proposal adds `maxSurge` support to both `ServingGroupRollingUpdate` and `RoleRollingUpdate`, allowing the ModelServing controller to create temporary updated replicas before removing outdated replicas and thereby reducing service disruption.

Unlike a Deployment, ModelServing does not create a new ReplicaSet for every revision, and unlike LeaderWorkerSet, it does not create one child workload CR per ServingGroup; instead, a single ModelServing CR directly owns Pods, PodGroups and the optional service while the in-memory datastore reconstructs ServingGroup and Role state from those resources, so rollout progress cannot be represented by desired replica counts on old and new child workloads.

This proposal introduces a surge pool whose replicas use ordinals above the current desired replica boundary, remain available throughout the rolling update, and are deleted together only after all desired stable replicas have been updated and become ready, with the controller reconstructing rollout progress from current desired replicas, resource ordinals, revisions, hashes, and readiness after restart while the datastore remains only a cache.

### Motivation

ModelServing currently controls disruption with `maxUnavailable`, requiring the controller to delete an outdated ServingGroup or Role instance before recreating its stable ordinal, which either limits rollout concurrency when the unavailable budget is small or reduces inference capacity when it is large.

Users with spare cluster capacity should be able to provision updated ServingGroups or Roles first, verify readiness, and only then replace outdated stable replicas while the controller continues to bound both additional resource usage and unavailability.

#### Goals

- Add absolute and percentage-based `maxSurge` configuration for `ServingGroupRollingUpdate`.
- Add independently calculated, per-Role `maxSurge` configuration for `RoleRollingUpdate`.
- Keep the number of live replicas within the configured surge budget.
- Continue enforcing `maxUnavailable` while surge replicas are created and stable replicas are replaced.
- Preserve stable ServingGroup and Role ordinals after the rollout completes.
- Preserve partition protection for stable ordinals.
- Recover active surge operations after controller restart or leader failover from desired replica counts, ordinals, revisions, hashes, and readiness without adding rollout-specific resource labels.
- Keep existing behavior unchanged when `maxSurge` is omitted or set to zero.

#### Non-Goals

- Redesigning ControllerRevision or adding user-facing rollback support.
- Defining traffic routing, connection draining, or request-level readiness semantics for old and new replicas.
- Changing autoscaling semantics or allowing an autoscaler to count temporary surge replicas as desired replicas.
- Persisting the complete controller datastore.
- Guaranteeing that a surge can be scheduled when cluster quota, accelerator capacity, or gang-scheduling requirements cannot be satisfied.

### Proposal

Add `maxSurge` to the shared rolling update configuration, applying its top-level value to the ModelServing replica count for `ServingGroupRollingUpdate` and each Role's inline value to that Role's replica count for `RoleRollingUpdate`.

A replica is classified as either:

- **Stable**: a replica whose ordinal is part of the desired stable ordinal set.
- **Surge**: a temporary updated replica created above the stable ordinal range as part of a reusable capacity pool that remains available while the stable replica set is upgraded.

Surge replicas form a reusable temporary capacity pool rather than one-to-one replacement replicas, so the controller creates the pool once, retains it while outdated stable ordinals are recreated with the updated revision, and deletes the entire pool only after every desired non-protected stable replica in the scope is updated and ready, retaining predictable names, partition boundaries, and scale-down behavior while avoiding repeated GPU allocation, model initialization, and deletion.

#### User Stories

##### Story 1: ServingGroup create-before-delete update

A user runs four ServingGroups with `maxUnavailable: 0` and `maxSurge: 1`, so when the template changes the controller creates one temporary updated ServingGroup, retains it while all four stable ordinals are upgraded in turn, and deletes it only after every stable ServingGroup is updated and ready, thereby initializing surge capacity only once while keeping at most five live ServingGroups and at least four ready ServingGroups when the workload and cluster are healthy.

##### Story 2: Per-Role surge update

A ServingGroup contains `prefill` and `decode` Roles but only `decode` changes, so with `decode.maxSurge: 1` and `decode.maxUnavailable: 0` the controller creates one temporary updated `decode` Role without duplicating `prefill`, retains it while every stable `decode` ordinal is upgraded, and removes it only after all stable `decode` Roles are updated and ready.

#### Notes/Constraints/Caveats

- Surge is bounded rather than guaranteed capacity, so a rollout waits if the cluster cannot schedule a surge replica.
- A ServingGroup surge duplicates all Roles in that ServingGroup and may cost significantly more than a Role surge, but retaining the pool until rollout completion avoids paying that allocation and initialization cost repeatedly.
- A Role surge is independently bounded for every Role in every ServingGroup.
- Gang scheduling must account for all Pods required by the surge unit, and the controller must not remove stable capacity merely because surge resources were created because the surge unit must first satisfy existing readiness rules.
- Partition applies to target stable ordinals, while temporary high ordinals do not move the partition boundary and are never partition-protected.
- Rollout budget changes do not represent workload template changes and must not produce a new ControllerRevision or Role template hash.

#### Risks and Mitigations

##### Resource pressure

Surge replicas may require substantial CPU, memory, accelerator, storage, and network resources for the full rolling-update duration, so the controller strictly limits live replicas to the resolved surge ceiling, defaults `maxSurge` to zero, and leaves Kubernetes quota and scheduling as the final resource admission controls, while retaining the pool avoids repeated allocation, model initialization, teardown, and the greater cumulative GPU occupancy caused by recreating surge capacity for every stable ordinal.

##### Gang-scheduling deadlock

A surge unit may remain pending when a complete PodGroup cannot be scheduled, so the controller retains stable replicas without consuming an unavailable slot until the surge is ready and uses events and status conditions to make the stalled rollout observable.

##### Incorrect classification after restart

The controller classifies replicas from the current desired count rather than persisted rollout labels, so for desired count $R$ every ordinal in `[0,R)` is stable and every ordinal greater than or equal to $R$ is surge, and restart reconstruction counts existing high ordinals before creating additional surge capacity to avoid duplicate pools.

##### Ordinal and name collisions

Temporary ordinals are allocated above all live stable and surge ordinals and remain reserved until deletion is observed, while deterministic ordinal-based names and validation of ModelServing ownership and revision on an AlreadyExists response prevent immediate allocation of another replica.

##### Incorrect readiness and status counts

Because existing readiness logic assumes that Role instance count equals the desired count, surge-aware calculations explicitly separate stable and surge instances, use only stable replicas for rollout completion and revision promotion, and use aggregate status to report actual live and ready capacity.

##### Spec changes during an active rollout

Changes to replicas dynamically move the stable boundary without invalidating the rollout, so scale-up promotes existing high ordinals into the stable range before creating new resources, while scale-down reclassifies ordinals outside the new stable range as surge or excess capacity and reconciles the new `replicas + maxSurge` ceiling before causing additional stable disruption.

##### Historical revision corruption

Because recreating a stable ordinal depends on immutable ControllerRevision data, the controller must never overwrite an existing revision name with the current template and must fail safely with an event instead of replacing stable capacity when stored data does not match the expected revision.

### Design Details

#### API

Extend `RollingUpdateConfiguration` with `maxSurge`:

```go
type RollingUpdateConfiguration struct {
  // MaxUnavailable limits unavailable stable replicas during rollout.
  // Percentages are resolved against the latest desired replicas and rounded down.
  // It defaults to 1 and may be 0 only when MaxSurge resolves above 0.
    MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

  // MaxSurge limits replicas created above the latest desired replicas.
  // Percentages are resolved against the latest desired replicas and rounded up.
  // It defaults to 0.
    MaxSurge       *intstr.IntOrString `json:"maxSurge,omitempty"`

  // Partition protects stable ordinals in [0, partition) from rollout.
  // Percentages are resolved against the latest desired replicas and rounded up.
    Partition      *intstr.IntOrString `json:"partition,omitempty"`
}
```

This is a target-state API whose implementation adds the field and kubebuilder markers to the source API and regenerates deepcopy code, client-go types, the Helm-embedded CRD, and CRD reference documentation before the YAML examples become usable by a cluster.

The field has these semantics:

- It accepts a non-negative integer or a percentage string.
- An integer is an absolute replica count.
- A percentage is resolved against the latest desired replica count on every reconcile and rounded up.
- The default is zero.
- The webhook rejects malformed values, negative values, and percentages above 100 percent.
- `maxUnavailable` and `maxSurge` cannot both resolve to zero for an updateable scope.

The current validator's unconditional rejection of `maxUnavailable: 0` changes to pairwise validation, making zero valid only when `maxSurge` resolves above zero, retaining the top-level default of one and existing omitted Role behavior, and requiring a positive Role `maxSurge` when Role `maxUnavailable` is explicitly zero.

For `ServingGroupRollingUpdate`, the relevant configuration is:

```yaml
spec:
 rolloutStrategy:
  type: ServingGroupRollingUpdate
  rollingUpdateConfiguration:
   maxUnavailable: 0
   maxSurge: 25%
```

For `RoleRollingUpdate`, the configuration remains inline on each Role:

```yaml
spec:
 rolloutStrategy:
  type: RoleRollingUpdate
 template:
  roles:
   - name: decode
     replicas: 4
     maxUnavailable: 0
     maxSurge: 1
```

Changing `maxSurge`, `maxUnavailable`, or `partition` changes rollout policy rather than the Pod template, so revision and Role template hash calculations continue to exclude these fields.

#### Budget Resolution

For a desired replica count $R$, the controller resolves:

$$
S = \operatorname{ResolveMaxSurge}(R)
$$

$$
U = \operatorname{ResolveMaxUnavailable}(R)
$$

Percentage `maxSurge` is rounded up while percentage `maxUnavailable` remains rounded down, producing the following two invariants:

$$
N_{live} \le R + S
$$

$$
N_{available} \ge R - U
$$

The creation budget is:

$$
B_{create} = \max(0, R + S - N_{live})
$$

The controller uses $B_{create}$ only to build or replenish the retained surge pool, never to replace and recreate the pool after each stable update, and it must verify before deleting an outdated stable replica that the resulting available count remains at least $R-U$, while a newly created but unready surge consumes surge capacity without contributing to availability.

The controller resolves $R$, $S$, $U$, and partition from the latest spec on every reconcile, so changing replicas during rollout intentionally changes the ordinal boundary and percentage budgets without requiring a persisted baseline.

#### Ordinal-Based Identity and Recovery

The controller does not add rollout-specific lifecycle, operation, generation, baseline replica, or scope labels and instead classifies each replica from its ordinal and the latest desired replica count.

For a ModelServing desired count $R$, ServingGroups with ordinal $o<R$ are stable and ServingGroups with ordinal $o\ge R$ are surge, while for a Role desired count $R_r$, Role instances with ordinal $o<R_r$ are stable and Role instances with ordinal $o\ge R_r$ are surge.

$$
Stable(R) = \{o \mid 0 \le o < R\}, \qquad Surge(R) = \{o \mid o \ge R\}
$$

The boundary is exclusive, so with `replicas: 4` ordinals 0 through 3 are stable and ordinal 4 and above are surge.

Existing names and labels already expose ServingGroup and Role ordinals, and existing revision and Role template hash labels identify whether each stable or surge replica uses the current or update template, so ownership, ordinal, revision, hash, resource existence, and readiness are sufficient to derive every rollout phase.

After restart, the controller rebuilds the datastore from informer-visible resources, partitions replicas at the latest desired boundary, fills missing stable ordinals before causing new disruption, counts existing high ordinals against the current surge ceiling, continues updating outdated stable replicas when any remain, and deletes high ordinals when all required stable replicas are updated and ready.

Changing replicas during rollout is a normal boundary transition: increasing $R$ promotes existing surge ordinals that enter `[0,R)` directly to stable replicas, while decreasing $R$ reclassifies ordinals outside `[0,R)` as surge candidates and removes the highest excess ordinals until live replicas satisfy the recalculated $R+S$ ceiling.

When a promoted replica already uses the update revision and is ready, it immediately contributes as an updated stable replica without recreation, while a newly reclassified surge replica that does not use the update revision cannot authorize stable deletion until it is updated and ready.

For example, increasing desired replicas from four to five immediately promotes existing surge ordinal 4 to stable ordinal 4 without deleting or recreating its Pods, after which the controller may create ordinal 5 to replenish the surge pool under the recalculated budget.

For example, decreasing desired replicas from five to three immediately reclassifies ordinals 3 and 4 as surge candidates, after which the controller deletes the highest excess ordinals until at most $3+S$ live replicas remain, may physically retain other high ordinals within that ceiling while reconciling them, and counts only update-revision, ready high ordinals as capacity that can authorize stable deletion.

No full rollout journal is added to ModelServing status because the desired boundary and existing revision fields provide the durable intent, ControllerRevisions provide historical templates, and child resources provide actual ordinal, revision, hash, and readiness state.

An active rollout is a derived predicate rather than persisted phase state: a ServingGroup scope is active when any desired stable ordinal is missing, outdated, or unready or when any high ordinal exists, and a Role scope is active under the equivalent Role-ordinal and Role-template-hash conditions.

#### ServingGroup Surge State Machine

For each ServingGroup rollout scope, reconciliation performs these idempotent phases and follows the same high-level create-surge, update-stable, and final-cleanup lifecycle used by LeaderWorkerSet:

1. **Create surge pool**: allocate up to the resolved surge budget as high group ordinals and create their ControllerRevisions if necessary, PodGroups, and all Role Pods with the update revision.
2. **Wait for usable surge capacity**: wait for sufficient surge ServingGroups to satisfy normal readiness before deleting stable capacity, while any unready surge consumes surge budget without contributing to availability.
3. **Upgrade stable ordinals**: repeatedly select outdated stable ordinals using the existing rollout ordering while excluding partition-protected ordinals, recheck `maxUnavailable`, delete each selected stable ServingGroup, recreate the same ordinal from the target ControllerRevision, and continue using the retained surge pool as available capacity.
4. **Wait for stable rollout completion**: retain the entire surge pool until every desired non-protected stable ServingGroup is on the target revision and ready and every protected stable ServingGroup remains healthy at its preserved revision.
5. **Delete surge pool**: delete all ServingGroup Pods, Services, and PodGroups whose ordinals remain outside the final desired stable range and clear their entries from the datastore after deletion is observed.

Normal `syncServingGroupReplicas` classifies ordinals against the latest `spec.replicas`, counts only ordinals below that boundary as stable, includes higher ordinals when enforcing the surge ceiling, and retains allowed high ordinals until stable rollout completion.

Ordinal-boundary filtering is mandatory before every ordinary scale-down or deletion-cost sort, including the replica synchronization stage that precedes rolling update management, so the existing reconcile order cannot reclaim an allowed surge ordinal as excess capacity before stable rollout completion.

With $R=4$ and `maxSurge=1`, one rollout creates temporary ordinal 4 once and retains it while stable ordinals 3 through 0 are upgraded:

| Phase | Stable ordinals | Surge ordinals | Live count |
| --- | --- | --- | --- |
| Initial | 0, 1, 2, 3 (old) | none | 4 |
| Surge pool ready | 0, 1, 2, 3 (old) | 4 (new) | 5 |
| Stable 3 replacing | 0, 1, 2 (old) | 4 (new) | 4 |
| Stable 3 ready | 0, 1, 2 (old), 3 (new) | 4 (new) | 5 |
| Stable 2 replacing | 0, 1 (old), 3 (new) | 4 (new) | 4 |
| All stable ready | 0, 1, 2, 3 (new) | 4 (new) | 5 |
| Final cleanup | 0, 1, 2, 3 (new) | none | 4 |

#### Role Surge State Machine

Role surge is calculated independently for each Role inside each ServingGroup, so Role $r$ with desired replicas $R_r$ uses these invariants:

$$
N_{live,r} \le R_r + S_r
$$

$$
N_{newAvailable,r} \ge R_r - U_r
$$

The phases mirror ServingGroup surge:

1. Create up to $S_r$ high Role ordinals with the updated Role template hash.
2. Wait for sufficient temporary Role entry and worker Pods to become ready before disrupting stable Role instances.
3. Retain the Role surge pool while repeatedly selecting outdated non-protected stable Role ordinals, rechecking the Role unavailable budget, deleting their Pods and Services, recreating the same stable ordinals with the updated template, and waiting for readiness as required by the budget.
4. Wait until every desired non-protected stable Role ordinal has the updated template hash and is ready and every protected stable Role remains healthy at its preserved revision.
5. Delete all Role instances and Services whose ordinals remain outside the final desired stable range, remove their PodGroup tasks, and complete the rollout.

Role readiness distinguishes desired stable Role instances from temporary surge instances, allowing the retained ready surge pool to contribute to availability throughout replacement without satisfying the requirement that every desired stable ordinal eventually exists at the updated Role template hash, and a ServingGroup returns to `Running` only after all stable Role updates are ready and the surge pool has been deleted.

The implementation replaces the current strict `len(roleList) == replicas` check with two explicit checks:

- **Stable readiness** requires every desired stable Role ordinal and all of its Pods to be ready.
- **Surge-unit readiness** requires the entry Pod and all worker Pods of one temporary Role instance to be ready.

Stable readiness determines when final surge-pool cleanup may begin, while surge-unit readiness authorizes deletion of outdated stable Roles and the resulting available count continues to control `maxUnavailable`.

When gang scheduling is enabled at ServingGroup scope, the PodGroup manager must include temporary Role tasks while they exist, remove them after surge cleanup, and reconcile idempotently across every phase.

For ServingGroup surge every high-ordinal ServingGroup owns its own PodGroup, whereas Role surge updates the existing ServingGroup PodGroup with high-ordinal Role tasks without creating a second PodGroup, with desired PodGroup membership derived from current replica boundaries and observed Role instances rather than only from `spec.template.roles`.

#### Partition Interaction

Partition protects stable ordinals in `[0, partition)`, and target selection uses parsed ordinals rather than datastore slice positions so missing low ordinals or temporary high ordinals cannot change protection.

All partition-sensitive controller paths, including template selection, scale-up gap filling, scale-down protection, and outdated-resource selection, must compare parsed stable ordinals with partition instead of using slice indexes or list positions.

- A protected stable ordinal is never selected as a surge replacement target.
- A temporary surge ordinal is not protected, even if list ordering changes.
- Filling a missing protected ordinal continues to use `CurrentRevision`.
- A scale-down moves the stable boundary downward, reclassifies higher ordinals, removes only replicas above the recalculated surge ceiling before new disruption, and does not change the revision of protected stable replicas.

#### Reconciliation Ordering

Because the current reconcile loop aligns desired replicas before managing rolling updates, both replica synchronization stages must become surge-aware:

- Stable reconciliation creates or removes only stable replicas needed by the desired spec.
- Replicas above the latest desired ordinal boundary are excluded from ordinary stable excess calculations while they fit within the surge ceiling.
- Surge reconciliation enforces the combined live ceiling and advances replacement operations.
- Headless Service and PodGroup synchronization handles stable and surge identity explicitly.
- Status is updated after all stages from one consistent datastore snapshot.

The controller may implement surge advancement as a dedicated stage before ordinary outdated-resource deletion or incorporate it into rolling update management, but both stages must use the same latest ordinal boundary and only one stage may initiate replacement for any stable target.

#### Status and Completion

`status.replicas` reports the actual number of live ServingGroups and may exceed `spec.replicas` during ServingGroup surge, `availableReplicas` may also temporarily exceed desired count, and Role surge leaves ServingGroup-level replica counts unchanged while keeping the ServingGroup progressing until Role operations finish.

`updatedReplicas` is calculated from desired stable replicas, while rollout completion, `CurrentRevision` promotion, and final ControllerRevision cleanup require both that all desired stable replicas satisfy their target revisions and readiness and that the surge pool has been deleted, so temporary surge replicas cannot complete or prematurely promote the rollout.

The controller emits events when a surge operation is created, becomes ready, starts stable replacement, completes, is cancelled, or cannot progress.

Status fields use the following counting scopes:

| Status field | Counting scope during surge |
| --- | --- |
| `replicas` | All live stable and surge ServingGroups. |
| `availableReplicas` | All ready stable and surge ServingGroups. |
| `currentReplicas` | Desired stable ServingGroups on `CurrentRevision`; excludes surge. |
| `updatedReplicas` | Desired stable ServingGroups on `UpdateRevision`; excludes surge. |
| `currentRevision` | Promoted only from the state of desired stable replicas. |
| `updateRevision` | Latest desired workload revision; a surge alone cannot promote it. |
| conditions | Progressing while any ServingGroup or Role surge operation is active. |

ControllerRevision cleanup uses revisions referenced by all live stable and surge replicas together with `CurrentRevision` and `UpdateRevision`, preventing deletion of any revision still needed to reconstruct rollout state.

#### Failure and Recovery

- **Surge Pod failure**: apply the existing recovery policy to the surge unit and do not delete stable capacity until the surge becomes ready again.
- **Manual surge deletion**: count the remaining high ordinals against the current surge budget and replenish the pool when stable replicas remain outdated, or finish without recreation when all stable replicas are already updated and ready.
- **All surge resources deleted after stable deletion**: restore every missing desired stable ordinal before selecting another outdated target and use `UpdateRevision` and the corresponding ControllerRevision to select its template.
- **Stable replica failure during replacement**: retain the ready surge pool and keep reconciling every missing or unready stable ordinal before advancing rollout or cleanup.
- **Controller restart**: scan owned resources, parse ordinals, revisions, hashes, and readiness, reconstruct datastore state using the latest desired boundaries, and resume from observed resources.
- **Stale events**: validate owner UID, ordinal classification, and revision before mutating reconstructed state.
- **Replica change**: immediately recalculate stable and surge ordinal ranges, promote existing surge replicas on scale-up, trim only excess high ordinals on scale-down, and continue rollout against the latest desired counts.
- **Template change during rollout**: treat the latest `UpdateRevision` and Role template hashes as the desired target, retain capacity within the recalculated ceiling, and reconcile existing stable and surge replicas toward the latest template.

#### Backward Compatibility

`maxSurge` defaults to zero, so existing ModelServing resources continue using the current delete-before-create behavior governed by `maxUnavailable` and existing API clients remain source compatible because the field is optional.

Adding the field requires regeneration of deepcopy code, client-go types, CRDs, Helm-embedded CRDs, and CRD reference documentation, while examples and rolling update documentation will include both rollout strategies.

#### Test Plan

### Alternatives

#### Store rollout state only in the datastore

The datastore could record a baseline replica count and explicit rollout phase, but this duplicates state that can be derived from the latest spec and live resource ordinals and would be lost on restart, so the datastore remains a reconstructable cache.

#### Store a complete rollout journal in ModelServing status

A status journal could persist a baseline replica count and every rollout phase as an explicit source of truth, but it would introduce frequent status writes, conflict retries, and duplicated state even though current desired counts, ordinals, revisions, hashes, and readiness are sufficient to reconstruct progress, so correctness does not depend on a rollout journal.

#### Keep surge ordinals as permanent replacements

The controller could delete an outdated low ordinal and retain the updated high ordinal to avoid recreating the stable ordinal, but the resulting holes and ever-increasing ordinals would complicate partition semantics, stable service names, scale-down ordering, and operational debugging, so temporary surge ordinals are preferred.

#### Continue using only maxUnavailable

This existing behavior remains available with `maxSurge: 0` but cannot provide create-before-delete updates when no unavailable capacity is acceptable.

#### Implement ServingGroup surge before Role surge

A phased implementation would reduce the first change's scope because Role readiness and PodGroup accounting require additional work, but both scopes are specified here so they share one API, identity model, budget semantics, and recovery guarantees.
