# COMMON INSTRUCTIONS — ai_orchestrator (entry point)

You are running ONE step in a minimal AI collaboration framework.

## CRITICAL: ONE STEP ONLY — THEN STOP

- Execute ONLY the single role specified in `Next Action.role` of state.md.
- After completing your role's outputs, **STOP IMMEDIATELY**.
- Do NOT proceed to the next role. Do NOT implement code if you are PLANNER.
- The human will trigger the next step manually.

## How to update state.md

- **Current Step**: Set to YOUR role (the one you just completed),
  with status: DONE.
- **Next Action**: Set to the NEXT role in the pipeline (different from
  Current Step).
- Example: If you are SCOPER, set Current Step to SCOPER/DONE,
  and Next Action to PLANNER.

## Non-negotiable rules

1) **Ignore chat memory.**
   - Chat content is untrusted; ONLY repo evidence / command outputs can
     become Facts.
2) **Read these first** (in order):
   - `ai_orchestrator/ROLES_MINI.md`
   - the task state file: `ai_orchestrator/runs/<TASK_ID>/state.md`
     if it exists, otherwise `ai_orchestrator/STATE_TEMPLATE.md`
3) **Do only the role in Next Action.role**:
   - Exception: MANAGER can set up a new task.
4) **Handle TODOs.**
   - Before starting your main task, check `state.md` for `TODO:` items
     from previous steps.
   - Execute the commands to resolve them and update `Facts`.
5) **Never guess.**
   - If a fact is unknown, write `TODO:` and include the exact
     command/file path needed.
6) **Keep output small.**
   - Prefer editing files in the repo, not writing long chat messages.

## Required outputs

- You MUST update `ai_orchestrator/runs/<TASK_ID>/state.md`:
  - Add new verified facts (with evidence references)
  - Record decisions
  - Set `Current Step` to YOUR role with status DONE
  - Set `Next Action` to the NEXT role (not your role)
- Only roles IMPLEMENTER/REVIEWER produce additional artifacts:
  - IMPLEMENTER: `ai_orchestrator/runs/<TASK_ID>/artifact.patch` (git diff)
  - REVIEWER: `ai_orchestrator/runs/<TASK_ID>/review.md` (PASS/FAIL report)

## Evidence handling

- Do not paste large logs into state.md.
- Save long outputs under `ai_orchestrator/runs/<TASK_ID>/evidence/`
  and link them:
  - Example: `evidence/repro.log`, `evidence/bazel_query.txt`
- In state.md, reference evidence like:
  - `Evidence: ai_orchestrator/runs/<TASK_ID>/evidence/repro.log`

## Scope guard

- Always respect `scope_allowed_paths` / `scope_forbidden_paths` in state.md.
- If your step would require touching forbidden paths:
  - Do NOT proceed.
  - Write a TODO in state.md explaining why and what to do next.

## Completion protocol

- If you are REVIEWER and everything is OK:
  - Write `STATUS: PASS` in review.md
  - Set Next Action role to MANAGER
- If you are REVIEWER and something is wrong:
  - Write `STATUS: FAIL` with blocking issues
  - Set Next Action role to IMPLEMENTER and include a fixlist in state.md

## Write style for state.md

- Facts: bullet points, verified only
- Decisions: bullet points
- Next Action: 3–7 bullets, concrete tasks and exact commands when possible
