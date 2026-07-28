---
title: Role-Based Rolling Update
authors:
- "@LiZhenCheng9527" # Authors' GitHub accounts here.
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-03-09

---

## Role-Based Rolling Update

<!--
This is the title of your proposal. Keep it short, simple, and descriptive. A good
title can help communicate what the proposal is and should be considered as part of
any review.
-->

### Summary

<!--
This section is incredibly important for producing high-quality, user-focused
documentation such as release notes or a development roadmap.

A good summary is probably at least a paragraph in length.
-->

This proposal will outline the shortcomings of ServingGroup Rolling Updates and the necessity of Role Rolling Updates, along with how to implement Role Rolling Updates.

### Motivation

<!--
This section is for explicitly listing the motivation, goals, and non-goals of
this proposal.  Describe why the change is important and the benefits to users.
-->

At present, modelServing supports ServingGroup Rolling Updates. Any changes to the content within Spec.Template.Roles will trigger a rolling update. During a ServingGroup Rolling Update, the controller first scales up or updates the desired ServingGroups and only then deletes outdated ServingGroups, subject to the configured `maxUnavailable`. However, rolling the entire ServingGroup for large models still consumes significant resources and time because old and new ServingGroups coexist during the transition. The ReCreatePolicy and roleRecreate were introduced for similar reasons.

Therefore, for roles, rolling updates must also be maintained continuously. This avoids triggering a full ServingGroup rolling update when only a single role changes, thereby reducing unnecessary resource consumption and update churn.

#### Goals

<!--
List the specific goals of the proposal. What is it trying to achieve? How will we
know that this has succeeded?
-->

Support for role-based rolling updates:

- Support Partition. Partitioning is still supported at the serving group level. However, even in role-based rolling updates, partition ensures that some servingGroups remain unchanged.
- Support ControllerRevision history for Roles. Maybe servingGroup ControllerRevision is enough.
- Support two-level MaxUnavailable. The top-level rollout configuration limits
    unavailable ServingGroups, while each Role can independently limit unavailable
    role replicas inside a selected ServingGroup.
- Perform a rolling update step by step in descending order of Role IDs.

#### Non-Goals

<!--
What is out of scope for this proposal? Listing non-goals helps to focus discussion
and make progress.
-->

- Support MaxSurge
- Rollback is not supported at this stage. Support is planned for future implementation.

### Proposal

<!--
This is where we get down to the specifics of what the proposal actually is.
This should have enough detail that reviewers can understand exactly what
you're proposing, but should not include things like API designs or
implementation. What is the desired outcome and how do we measure success?.
The "Design Details" section below is for the real
nitty-gritty.
-->

#### User Stories (Optional)

<!--
Detail the things that people will be able to do if this proposal is implemented.
Include as much detail as possible so that people can understand the "how" of
the system. The goal here is to make this feel real for users without getting
bogged down.
-->

##### Story 1

In PD separation scenarios where only prefill or decode requires upgrading, deleting the entire ServingGroup results in resource wastage.

##### Story 2

During subsequent rolling updates of the ServingGroup, distinctions can be made based on each role's status to determine whether the entire ServingGroup should be updated or whether an update should be performed for a single role.

#### Notes/Constraints/Caveats (Optional)

<!--
What are the caveats to the proposal?
What are some important details that didn't come across above?
Go in to as much detail as necessary here.
This might be a good place to talk about core concepts and how they relate.
-->

#### Risks and Mitigations

<!--
What are the risks of this proposal, and how do we mitigate?

How will security be reviewed, and by whom?

How will UX be reviewed, and by whom?

Consider including folks who also work outside the SIG or subproject.
-->

### Design Details

<!--
This section should contain enough information that the specifics of your
change are understandable. This may include API specs (though not always
required) or even code snippets. If there's any ambiguity about HOW your
proposal will be implemented, this is the place to discuss them.
-->

Add a new RolloutStrategyType: RoleRollingUpdate in the modelServing API.

```go
type RolloutStrategyType string

const (
    // ServingGroupRollingUpdate indicates that ServingGroup replicas will be updated one by one.
    ServingGroupRollingUpdate RolloutStrategyType = "ServingGroup"

    // RoleRollingUpdate indicates that Role replicas will be updated one by one.
    RoleRollingUpdate RolloutStrategyType = "Role"
)
```

Include the role's revision within the role status in the datastore. This revision is calculated by computing the hash value of the role's specific configuration.

```go
type Role struct {
    Name     string
    // This is servingGroup revision
    Revision string
    // This is role revision
    RoleRevision string
    Status   RoleStatus
}
```

Within the `manageServingGroupRollingUpdate` function in the `ModelServing` controller, add handling for `RolloutStrategyType == RoleRollingUpdate`.

```go
get maxUnavailable and another rolling update configuration
for _, role := range servingGroup.Spec.Template.Roles {
    get role old RoleRevision from datastore
    if hash(role) != roleOldRevision {
        calculate maxScaleDown
        delete maxScaleDown replicas of role
    }
}
```

#### ServingGroup-level update budget

`spec.rolloutStrategy.rollingUpdateConfiguration.maxUnavailable` applies to both
`ServingGroupRollingUpdate` and `RoleRollingUpdate`. For Role rolling updates it
limits how many ServingGroups can be unavailable at the same time. The controller
calculates the number of ServingGroups that may be processed as:

```text
minAvailable = max(0, spec.replicas - maxUnavailable)
maxScaleDown = max(0, observedServingGroups - minAvailable - newServingGroupUnavailable)
```

The controller selects the groups for the current reconciliation before
performing any deletion. Existing `Rolling` groups are selected first and are
always continued because their updates are already in progress. The remaining
slots are filled first with outdated non-running groups and then with outdated
running groups. Within each category, groups with larger ordinals are selected
first. Unavailable groups already on the new revision reduce `maxScaleDown`.
Old-revision non-running groups do not reduce it, but each selected group still
occupies one processing slot for the current reconciliation.

The top-level `partition` also applies to both strategies and protects
ServingGroups whose ordinals are lower than the partition. Role-level
`maxUnavailable` and `partition` remain a second, independent limit within each
selected ServingGroup.

When a ServingGroup is selected for Role rolling update, its datastore status is
changed from `Running` to `Rolling`. It remains `Rolling` across all Role update
batches. The status changes back to `Running` only after no unprotected outdated
Role remains and all Role replicas are ready. This prevents a temporarily ready
batch from releasing the ServingGroup-level availability slot too early.

For `ServingGroupRollingUpdate`, the ServingGroup also remains `Rolling` while
its resources are deleted. The deletion handlers treat this as whole-group
deletion in that strategy, while `RoleRollingUpdate` continues to use `Rolling`
for role-level replacement without deleting the ServingGroup itself.

Rollback is out of scope for this proposal and may be addressed in a future enhancement.

#### Test Plan

<!--
**Note:** *Not required until targeted at a release.*

Consider the following in developing a test plan for this enhancement:
- Will there be e2e and integration tests, in addition to unit tests?
- How will it be tested in isolation vs with other components?

No need to outline all test cases, just the general strategy. Anything
that would count as tricky in the implementation, and anything particularly
challenging to test, should be called out.

-->

- Add unit tests for main functions
- Verify that Role rolling updates respect the top-level ServingGroup
    `maxUnavailable` and `partition` together with Role-level limits.
- Add E2E tests for RoleRollingUpdate

### Alternatives

<!--
What other approaches did you consider, and why did you rule them out? These do
not need to be as detailed as the proposal, but should include enough
information to express the idea and why it was not acceptable.
-->

<!--
Note: This is a simplified version of kubernetes enhancement proposal template.
https://github.com/kubernetes/enhancements/tree/3317d4cb548c396a430d1c1ac6625226018adf6a/keps/NNNN-kep-template
-->