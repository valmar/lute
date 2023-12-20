# [ADR-5] Task-Executor IPC is Managed by Communicator Objects

**Date:** 2023-12-06

## Status
**Proposed**

## Context and Problem Statement
- A form (or forms) of inter-process communication needs to be standardized between Task subprocesses and executors.
- Signals need to be sent potentially bidirectionally.
- Results need to be retrieved from the Task in a generic manner.

## Decision
`Communicator` objects which maintain simple `read` and `write` mechanisms for `Message` objects. These latter can contain arbitrary Python objects. `Task`s do not interact directly with the communicator, but rather through specific instance methods which hide the communicator interfaces. Multiple Communicators can be used in parallel. The same `Communicator` objects are used identically at the `Task` and `Executor` layers - any changes to communication protocols are not transferred to the calling objects.

### Decision Drivers
* `Task` output needs to be routed to other layers of the software, but the `Task`s themselves should have no knowledge of where the output ends up.
* Ideally have the ability to send arbitrary objects (strings, arrays, objects, ...)
  * Ideally not limited by size of the transferred object
* Communication should be hidden from callers - "somewhat more declarative than imperative."
* Ability for protocols to be swapped out, or trialled without significant rewrites.
* Must handle uncontrolled output from "Third-party" software as well as "in-house" or "first-party" communication which is directly managed.

### Considered Options
* Singular specific options:
  * Relying solely on pipes over stdout/stderr
    * These are already controlled when the Executor opens the `subprocess`
    * Unfortunately, the pipe buffer is limited, and processes may hang when the output is too large (~64k or lower depending on machine)
  * Using a separate IPC method (e.g. Sockets)
    * "Binary" or "Third-party" tasks would have no communication captured at all, and while signalling is not possible in the same way with these tasks, some output must still be captured and routed.
* Direct management of multiple communication methods
  * E.g. use a combination of pipes and sockets, directly managed by the `Task` and `Executor` layers.

### Communicator Types
* `Communicator` : Abstract base class - defines interface
* `PipeCommunicator` : Manages communication through pipes (`stderr` and `stdout`)
* `SocketCommunicator` : Manages communication through Unix sockets

## Consequences
* Complexity due to management of (potentially) multiple communication methods
  * Some of this compelxity is isolated, however, to a single object.
* From the `Task` and `Executor` side, IPC is greatly simplified
  * Management is delegated to the `Communicator`
* Communication is "pluggable" -> not limited by the advantages and disadvantages of any single communication method or protocol
* Arbitrary objects can be sent and received
  * Limits on size or type of object should not be an issue (e.g. large results output can be handled)

## Compliance
* Communication is handled in base classes.
* `Communicator` objects are non-public. Their interfaces (already limited) are handled by simple methods in the base classes of `Task`s and `Executor`s.
  * The `Communicator` should have no need to be directly manipulated by callers (even less so by users)

## Metadata
- This ADR WILL be revisited during the post-mortem of the first prototype.
- Compliance section will be updated as prototype evolves.
