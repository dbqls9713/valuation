# ai_orchestrator

Minimal AI collaboration framework where each model performs one step,
passing state via files.

## Core Principles

1. **One step per model** — Each model executes only the current step
   in `state.md`.
2. **State is the single source of truth** — Never rely on chat memory.
3. **Human triggers each step** — User decides when to invoke the next model.
4. **Scope guard** — Explicit allowed/forbidden paths prevent unintended
   changes.

## Structure

```text
ai_orchestrator/
├── README.md              # This file
├── COMMON_INSTRUCTION.md  # Entry point for all models
├── ROLES_MINI.md          # Role definitions
├── STATE_TEMPLATE.md      # Template for new tasks
└── runs/
    └── <TASK_ID>/
        ├── state.md       # Current task state
        ├── artifact.patch # Code changes (IMPLEMENTER)
        ├── review.md      # Review result (REVIEWER)
        └── evidence/      # Logs, outputs, etc.
```

## State Flow

```text
MANAGER → SCOPER → PLANNER → IMPLEMENTER → REVIEWER
                                   ↑              │
                                   └── (FAIL) ────┘
                                         │
                                   (PASS) → MANAGER
```

## How to Trigger

1. **Start a new task (MANAGER)**

   ```text
   please start a new task with MANAGER role:
   "valuation/policies/fade.py 에서 발생하는 division by zero 버그 수정"
   (TASK_ID: 20260103-fix-fade-division)
   ```

2. **Continue with the common command**

   ```text
   ai_orchestrator/runs/20260103-fix-fade-division/state.md 읽고
   Next Action 수행해줘
   ```

3. **Repeat step 2** until **MANAGER reports completion** — Task is done.

### Tips

- You can use the same model for all steps, or switch models.
- The common command works because each step writes `Next Action.role`
  in `state.md`.
- If a step is blocked, `state.md` will contain `TODO:` items with exact
  commands to resolve.

## Adding a New Role

To extend the framework with a new role:

### 1. Define the role in `ROLES_MINI.md`

Add a new section following this template:

```markdown
---

## ROLE: <ROLE_NAME> (<one-line purpose>)

Goal: <What this role achieves>

Must do:

1) <Required action 1>
2) <Required action 2>
3) Write Next Action for <NEXT_ROLE>.

Must NOT do:

- <Forbidden action 1>
- <Forbidden action 2>

Outputs:

- Update `ai_orchestrator/runs/<TASK_ID>/state.md`
- (Optional) <artifact files>
```

### 2. Update `STATE_TEMPLATE.md`

Add the new role to the step enum:

```markdown
# Current Step

- step: <SCOPE|PLAN|IMPLEMENT|REVIEW|NEW_ROLE>
```

### 3. Update the State Flow diagram

In `ROLES_MINI.md`, update the flow to include the new role:

```markdown
## State Flow

MANAGER → SCOPER → PLANNER → NEW_ROLE → IMPLEMENTER → REVIEWER
```

### 4. (Optional) Define new artifacts

If your role produces artifacts (like `review.md` for REVIEWER):

- Document the artifact path in the role's `Outputs` section.
- Update `COMMON_INSTRUCTION.md` to mention the new artifact.

### Example: Adding a TESTER role

```markdown
## ROLE: TESTER (Verify implementation before review)

Goal: Run comprehensive tests and document results.

Must do:

1) Run all tests in scope (unit, integration).
2) Document test results with evidence.
3) If tests fail, return Next Action to IMPLEMENTER.
4) If tests pass, write Next Action for REVIEWER.

Must NOT do:

- No code changes.
- No skipping failing tests.

Outputs:

- Update `ai_orchestrator/runs/<TASK_ID>/state.md`
- `ai_orchestrator/runs/<TASK_ID>/test_results.log`
```
