# Milestone A Local MVP Flow (Core ↔ TARS)

This runbook wires and validates the local poll/callback loop for Milestone A using:
- Core worker endpoints:
  - `GET /api/v1/workers/tasks/poll?task_type=remote-handshake`
  - `POST /api/v1/workers/tasks/{task_id}/result`
- TARS poll worker runtime (`ui_tars.executor.worker`)
- Mock execution mode (`MESHCENTRAL_MOCK=1`)

## Best local wiring approach

For this repository, the most reliable local dev wiring is a **repo-local run script + runbook**:
- no external compose dependency required,
- deterministic seeded task and audit capture,
- validates endpoint shape compatibility and callback payload.

Script: `codes/scripts/local_milestone_a_mvp.py`

## What the script proves

1. Seeds a `PENDING` `remote-handshake` task in local Core stub state.
2. Starts local Core stub with the locked poll and result endpoints.
3. Starts TARS worker polling loop.
4. Worker claims task (`IN_PROGRESS`).
5. Worker executes mock handshake (`MESHCENTRAL_MOCK=1`).
6. Worker posts result callback to Core.
7. Core stores `COMPLETED` state, evidence URIs, and audit events.

## Run commands

```bash
cd codes
PYTHONPATH=. MESHCENTRAL_MOCK=1 python scripts/local_milestone_a_mvp.py
```

Expected output includes:
- `Milestone A local MVP flow OK`
- final task status `COMPLETED`
- audit events containing `task.claimed` and `task.completed`
- result excerpt with evidence `s3://aillium-evidence/...` URIs and worker id.

## Contract compatibility notes

The worker callback payload includes both snake_case and camelCase for worker/task IDs to tolerate Core validator differences:
- `worker_id` and `workerId`
- `task_id` and `taskId`

Payload keeps existing shared executor response semantics (`status`, `result`, `error`, artifacts) from the reusable handshake implementation.
