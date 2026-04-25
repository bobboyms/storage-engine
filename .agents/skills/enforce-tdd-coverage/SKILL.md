---
name: enforce-tdd-coverage
description: Enforce test-driven development and a minimum 70% total coverage gate for this Go project. Use automatically for every code implementation, bug fix, refactor, behavior change, API change, storage engine change, or test-related task in this repository before editing production code.
---

# Enforce TDD Coverage

## Overview

Apply TDD as the default workflow for code changes in this repository. Require explicit Red, Green, Refactor evidence and require total Go coverage to remain at or above 70%.

## TDD Cycle

Follow this cycle for every behavior change, bug fix, refactor, or implementation task:

1. Red: write or update a test that fails because the requested functionality or fixed behavior does not exist yet.
2. Green: write the minimum production code necessary to make that test pass.
3. Refactor: improve organization, clarity, performance, naming, duplication, or structure without breaking the passing tests.

Do not skip Red or Green. Do not treat Refactor as optional when the first passing implementation leaves obvious duplication, unclear names, unnecessary complexity, or inefficient code.

## Workflow

1. Inspect the target package and nearby tests before editing production code.
2. Red: write or update the smallest meaningful failing test that captures the requested behavior or bug.
3. Run the focused test command and confirm it fails for the expected reason.
4. Green: implement the minimal production change needed to pass the test.
5. Re-run the focused test until it passes.
6. Refactor: improve the code while preserving the passing tests, then re-run the focused test or relevant package suite.
7. Run the relevant package suite.
8. Run the full coverage gate before final response:

```bash
go test ./... -coverprofile=/tmp/go_project_coverage.out
go tool cover -func=/tmp/go_project_coverage.out
```

9. If total coverage is below 70.0%, add meaningful tests for changed or adjacent behavior before finishing.

## Rules

- Prefer focused tests near the changed code over broad integration tests, unless the behavior crosses package boundaries.
- Do not implement production code before creating or updating the failing test, except for pure documentation, formatting, or non-behavioral metadata changes.
- Keep each Red, Green, Refactor cycle small. For multiple behaviors, repeat the cycle per behavior instead of writing a large mixed change.
- Do not lower coverage thresholds, delete tests, skip tests, weaken assertions, or add artificial tests that do not verify behavior.
- Use `/tmp` for ad hoc coverage profiles to avoid repository churn.
- Include the Red failure command/result, Green passing command/result, refactor verification command/result, full coverage command, and final total coverage in the final response.
- If a command cannot run because of environment limits, state that explicitly and include the exact command that should be run.

## Project Commands

Use these defaults unless the surrounding code indicates a narrower or more appropriate command:

```bash
go test ./pkg/<package> -run <TestName> -count=1
go test ./pkg/<package> -count=1
go test ./... -coverprofile=/tmp/go_project_coverage.out
go tool cover -func=/tmp/go_project_coverage.out
```

For changes touching chaos, faults, stress, race behavior, or durability semantics, also run the relevant Makefile target when feasible: `make test-race`, `make test-chaos`, `make test-faults`, `make test-stress`, or `make test-safety`.
