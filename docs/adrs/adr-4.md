# [ADR-4] Airflow `Operator`s and LUTE `Executor`s are Separate Entities

**Date:** 2023-11-06

## Status
**Proposed**

## Context and Problem Statement
- Airflow operators submit tasks by calling the JID API
  - This is required since tasks running where Airflow is running would not have access to the data
- The current plan (cf. ADR-1 and ADR-2) requires submission of the `Executor` which in turn submits the `Task`
  - Under this plan the Executor must be separated from the Airflow operator.

## Decision
### Decision Drivers
*

### Considered Options
*

## Consequences
*

## Compliance

## Metadata
