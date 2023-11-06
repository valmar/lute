# [ADR-1] All Analysis Tasks Inherit from a Base Class

**Date:** 2023-11-06

## Status
**Accepted**

## Context and Problem Statement
- Analysis programs of interest have varied APIs.
- Nonetheless, for the purposes of this software, a unified interface is desirable.
- Providing a unified interface can be simplified by inheritance from a base class for all analysis activites.

## Decision
### Decision Drivers
* The original `btx` tasks had heterogenous interfaces.
  * This makes debugging challenging due to the need to look-up or remember different methods of task handling.
* The need to provide modular access to various types of software.
* A desire to reduce code redundancy for implementation decisions which affect ALL tasks.
* A need to provide access to/wrap third-party binaries.

### Considered Options
* Tasks as functions, with common interfaces provided through decorators, etc.
* Task code as functions wrapped by the execution code (cf. ADR-2)

## Consequences
* Simplified package structure.
* Ability to push feature updates to all `Task`s simultaneously.
* Potential complications due to an additional layer of abstraction.

## Compliance
* Data validation and type checking performed
* Common calling interface at higher levels relies on class structure (Execution layers, cf. ADR-2)

## Metadata
- This ADR WILL be revisited during the post-mortem of the first prototype.
- Compliance section will be updated as prototype evolves.
