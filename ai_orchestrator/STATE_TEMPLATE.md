# Task State

## Task

- id: TASK_ID
- goal: one line
- scope_allowed_paths:
  - ...
- scope_forbidden_paths:
  - ...

## Current Step (the role that was just completed)

- role: MANAGER|SCOPER|PLANNER|IMPLEMENTER|REVIEWER
- status: DONE

## Next Action (for the NEXT model — must differ from Current Step)

- role: MANAGER|SCOPER|PLANNER|IMPLEMENTER|REVIEWER
- what_to_do:
  - bullet 1
  - bullet 2
  - bullet 3
- required_outputs:
  - file path
- exact_commands_to_run (if any):
  - ...

## Facts (verified only)

- ...

## Decisions

- ...

## Pipeline Record (fill as needed)

### Repro

- ...

### Observations

- ...

### Hypotheses (2–3)

- ...

### Experiments

- ...

### Fix and Regression test

- ...
