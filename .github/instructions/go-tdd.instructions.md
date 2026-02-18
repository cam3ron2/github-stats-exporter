---
description: "Test-driven development workflow guidance with test-first rules, table-driven cases, and exhaustive coverage expectations"
applyTo: "**/*.go,**/*_test.go"
---

## Test-driven development (TDD) instructions

Use a test-first workflow for all behavioral changes. Tests must be written before code contributions, and the production code should only be added or changed after the test describes the expected behavior.

## TDD workflow

1. **Red**: Write a failing test that describes the desired behavior in clear, executable terms.
2. **Green**: Implement the smallest code change needed to make the test pass.
3. **Refactor**: Improve structure and readability while keeping tests green.
4. **Repeat**: Add the next test to expand coverage until the behavior is complete.

## Test-first rules (mandatory)

- Write tests **before** implementing or modifying production code.
- Keep tests behavior-focused; avoid testing private implementation details.
- Prefer deterministic tests with explicit inputs/outputs and no reliance on wall-clock time or external services.
- If a change is purely refactoring, ensure existing tests pass and add tests only when behavior changes.

## Table-driven testing requirements

- Use table-driven tests wherever multiple input/output permutations exist.
- Each test case must include a descriptive `name` and clearly specified inputs and expected outputs.
- Keep test tables readable and grouped by scenario (happy path, boundary, error, concurrency).

## Exhaustive test case expectations

Cover the full behavior surface, including:

- **Happy paths**: valid inputs and expected outputs.
- **Boundaries**: min/max values, empty values, and off-by-one cases.
- **Errors**: invalid inputs, missing dependencies, and expected failures.
- **State transitions**: role changes, retries, and idempotency.
- **Concurrency**: parallel access and race conditions where applicable.

Do not ship a change without tests that exercise all relevant branches and error paths.

## Test quality standards

- Tests must be deterministic and repeatable.
- Each test should assert a single logical behavior (avoid mega-tests).
- Keep test setup minimal; use helper functions or fixtures when setup repeats.
- Use explicit time control (inject clocks or use `time.Now` indirection) instead of `time.Sleep`.

## Review checklist

- [ ] A failing test was added before code changes (Red).
- [ ] The minimal change made the test pass (Green).
- [ ] Refactoring kept tests green and improved readability (Refactor).
- [ ] Tests are table-driven where appropriate.
- [ ] Test cases are exhaustive for the behavior being added or changed.
