# ROLES (Mini) — One-step-per-model, state-driven

All roles must:

- Read `ai_orchestrator/COMMON_INSTRUCTION.md` first.
- Do ONLY the current step described in
  `ai_orchestrator/runs/<TASK_ID>/state.md` except for the MANAGER.
- Update `ai_orchestrator/runs/<TASK_ID>/state.md` with:
  Facts, Decisions, Next Action.
- Never rely on chat memory. Use repo evidence only.
- If unknown: write TODO + exact command/file path to verify.

State is the single source of truth.
Artifacts (patch/review/logs) must be saved to disk and referenced from state.

## State Flow

```text
MANAGER → SCOPER → PLANNER → IMPLEMENTER → REVIEWER
                                   ↑              │
                                   └── (FAIL) ────┘
                                         │
                                   (PASS) → MANAGER
```

---

## ROLE: MANAGER (Set the goal of the task)

Must do:

1) Set the goal of the task.
   If the goal is too big, split into 1–3 sub-tasks and pick one for this run.
2) Receive a new `TASK_ID` from the user and create a new `state.md` under
   `ai_orchestrator/runs/<TASK_ID>` based on `ai_orchestrator/STATE_TEMPLATE.md`
3) Write Next Action for SCOPER.
4) If the task is PASSED, report the result to the user.

Must NOT do:

- No code changes. No patch.
- No long explanations.

Outputs:

- Create a new `state.md` under `ai_orchestrator/runs/<TASK_ID>` only.

---

## ROLE: SCOPER (Scope guard)

Goal: Make the task doable and safe.

Must do:

1) Confirm/define allowed_paths and forbidden_paths (scope guard).
2) Identify in-scope components (dirs/targets) and what is out-of-scope.
3) If the request is big, split into 1–3 sub-tasks and pick one for this run.
4) Record any required repo facts as TODO + exact commands to get them.
5) Write Next Action for PLANNER:
   what hypothesis/plan should cover, what inputs are needed.

Must NOT do:

- No code changes. No patch.
- No long explanations.

Outputs:

- Update `ai_orchestrator/runs/<TASK_ID>/state.md` only.

---

## ROLE: PLANNER (Repro → Observe → Hypotheses → Experiments → Fix Plan)

Goal: Produce an executable debug plan with 2–3 falsifiable hypotheses.

Must do:

1) Repro: specify minimal failing test/command
   (or TODO + exact command to find it).
2) Observations: capture key error/log/stack/env facts
   (or point to evidence file).
3) Hypotheses: list 2–3 plausible root causes.
4) Experiments: for each hypothesis, define a validation experiment
   (change/measurement + expected signal).
5) Fix plan: minimal fix + required regression test plan.
6) Write Next Action for IMPLEMENTER with a checklist + exact commands to run.

Must NOT do:

- No code changes in this step.
- No guessing about versions/labels/targets.

Outputs:

- Update `ai_orchestrator/runs/<TASK_ID>/state.md`.
- (Optional) write evidence files under
  `ai_orchestrator/runs/<TASK_ID>/evidence/` and link them in state.

---

## ROLE: IMPLEMENTER (Execute experiments + Minimal fix + Regression test)

Goal: Turn the plan into verified changes with minimal diff.

Must do:

1) Follow PLANNER checklist: run repro, run experiments.
2) Apply the minimal fix DIRECTLY to the files. (Repo will be dirty)
3) Generate patch artifact: `git diff > .../artifact.patch`
4) Run verification commands and record results in state.md.
5) Generate patch artifact:
   - `git diff > ai_orchestrator/runs/<TASK_ID>/artifact.patch`
6) Write verification commands run + results summary.
7) Write Next Action for REVIEWER.

Must NOT do:

- No out-of-scope changes.
- No unrelated refactors.

Outputs:

- `ai_orchestrator/runs/<TASK_ID>/artifact.patch`
- Update `ai_orchestrator/runs/<TASK_ID>/state.md`
  (include verification commands/results and links to evidence).

---

## ROLE: REVIEWER (Red Team PASS/FAIL)

Goal: Independently validate scope safety, correctness, and pipeline compliance.

Must do:

1) Check scope: verify `artifact.patch` against forbidden paths.
2) Verify correctness: EXECUTE the verification commands left by IMPLEMENTER
   if possible.
   - claims vs repo evidence
   - commands make sense (tests vs binaries)
3) Check pipeline compliance:
   - Repro documented
   - Observations captured
   - Hypotheses (2–3) present
   - Experiments described
   - Regression test present
4) Write PASS/FAIL review artifact:
   - `ai_orchestrator/runs/<TASK_ID>/review.md` with STATUS: PASS|FAIL
     and blocking issues.
5) Update `ai_orchestrator/runs/<TASK_ID>/state.md`:
   - last_status: PASS|FAIL
   - Next Action: if FAIL → IMPLEMENTER with a fixlist; if PASS → MANAGER.

Must NOT do:

- No code changes.

Outputs:

- `ai_orchestrator/runs/<TASK_ID>/review.md`
- Update `ai_orchestrator/runs/<TASK_ID>/state.md`.
