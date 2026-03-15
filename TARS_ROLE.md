# aillium-tars role after OpenClaw runtime pivot

## Purpose (reduced)

`aillium-tars` exists to support migration safety and contract confidence, not to be the future runtime center.

## Responsibilities to keep

1. `executor.request` / `executor.response` contract validation against `aillium-schemas`.
2. Deterministic mock execution (`/executor/dry-run`) for integration testing and rollout checks.
3. Evidence/audit shaping used by downstream audit indexing during transition.
4. Short-term remote session handshake bootstrap (`/executor/remote-handshake`) when needed for transition operations.

## Responsibilities intentionally removed from strategy

- Runtime orchestration/planning and broad execution control (OpenClaw).
- Control-plane state and authoritative device/tenant records (Aillium Core).
- Remote-support substrate ownership (MeshCentral repository).
- Legacy push/inbound worker paths and other duplicated runtime/browser pathways.

## Deprecation policy in this repo

- Legacy endpoints `/executor/push` and `/executor/inbound` are explicitly deprecated.
- New work should not expand deprecated paths.
- Any runtime-like behavior should be treated as temporary unless justified as migration-critical.
