# [ADR-3] `Executor`s will run all `Task`s via subprocess

**Date:** 2023-11-06

## Status
**Proposed**

## Context and Problem Statement
- A mechanism is needed to submit `Task`s from within the `Executor` (cf. ADR-2)
- Ideally a single method can be used for all `Task`s, at all locations, but at the very least all `Task`s at a single location (e.g. S3DF, NERSC)

## Decision
### Decision Drivers
* Want to simplify the interface for `Task` submission, but have to submit both first-party and third-party code.
* Want to have execution/submission separated from the Task submission (cf. ADR-2)
* Need flexible method which can be used to run any task, and quickly adapted to new Tasks

### Considered Options
* Executor submits a separate SLURM job.
  * This strategy was employed by `JobScheduler` for `btx`
  * Challenging to maintain - non-trivial issues can arise, e.g. with MPI
* Use `multiprocessing` at the Python level.
  * More complex to manage
  * Provides more flexibility
* Different mechansims for third-party Task or first-party Tasks

## Consequences
* Communication must be via pipes or files
* Very challenging to share state between executor and task
  * Generally want to limit this, but makes certain communciation tasks harder (passing results e.g.)
* Easier to run binary (i.e. third party) tasks
* Simple to implement.
* Need a separate method (e.g. a single script) which is submitted as a subprocess
  * This script, e.g., will select the Task based on options provided by the Executor

## Compliance
* Implementation will be at base class level for the executors

## Metadata
