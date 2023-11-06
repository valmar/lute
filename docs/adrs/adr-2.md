# [ADR-2] Analysis Task Submission and Communication is Performed Via Executors

**Date:** 2023-11-06

## Status
**Accepted**

## Context and Problem Statement
- Analysis code should be independent of the location and manner it is run.
- Additionally, communication is required after task submission to understand task context/state/results.
  - This communication is best handled outside of the submitted job itself.
    - This provides a mechanism for continued communication even in the case of task failure.
- A separate Executor (Controller) provides a mechanism that allows for communication and job submission to be independent of the task code itself.
  - Therefore: An Executor will be submitted, which in turn submits the Task and manages communication activities.

## Decision
### Decision Drivers
* Removing the job submission and communication components from the `Task` code itself provides a separation of concerns allowing `Task`s to run indepently of execution environment.
  * A separate `Executor` can prepare environment, submission requirements, etc.
* A desire to reduce code redundancy. Providing unified interfaces through `Executor` classes avoids maintaining that code independently for each task (cf. alternatives considered).
  * Job submission strategies can be changed at the `Executor` level and immediately applied to all `Task`s.
  * If communication APIs change, this does not affect `Task` code.
* **Difficulties encountered due to edge-cases in the original `btx` tasks. E.g. task timeout leading to failure of a processing pipeline even if substantial work had been done and subsequent tasks could proceed.**
* Varied methods of `Task` submission already exist in the original `btx` but the methods were not fully standardized.
  * E.g. `JobScheduler` submission vs direct submission of the task.

### Considered Options
* Wrapping the execution, and communication, into the base `Task` class interface as pre/post analysis operations.
* Multiple `Task` subclasses for different execution environments.
* For communication specifically:
  * Periodic asynchronous communication in the `Task` class.

## Consequences
* `Task` code independent of execution environment.
* Ability to maintain communication even in the event of `Task` failure.
* Potential complications due to an additional layer of abstraction.

## Compliance
* Airflow will submit `Executor`s as the "Managed Task"
  * I.e. at the highlest API layer, `Task`s will not be submitted independently.

## Metadata
- This ADR WILL be revisited during the post-mortem of the first prototype.
- Compliance section will be updated as prototype evolves.
