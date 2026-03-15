# OpenClaw Pivot Inventory (aillium-tars)

## Still valuable and unique

- `codes/ui_tars/executor/dry_run.py`
  - Deterministic mock execution response generation.
  - Useful for schema-contract and migration validation.
- `codes/ui_tars/executor/schema_loader.py`
  - Runtime loading of canonical contracts from `aillium-schemas`.
- `codes/ui_tars/executor/audit.py`
  - Structured correlation/audit event emission.
- `codes/ui_tars/executor/server.py`
  - Minimal endpoint host that enforces request validation and routes transition-safe executor behavior.

## Temporarily useful during migration

- `codes/ui_tars/executor/remote_handshake.py`
  - Short-term remote handshake bridge to MeshCentral for migration operations.
- `codes/ui_tars/executor/meshcentral_client.py`
  - MeshCentral API adapter used by remote-handshake path.
- `codes/ui_tars/executor/meshcentral_mock.py`
  - Mock remote substrate for local/testing scenarios.

## Redundant because OpenClaw now covers it

- Any runtime/orchestration expectations implied by broad “executor as runtime center” messaging in legacy docs.
- Any path that suggests TARS should own browser/runtime orchestration beyond dry-run + handshake transition support.

## Obsolete legacy paths

- Legacy push/inbound worker paths (`/executor/push`, `/executor/inbound`) are now explicitly deprecated in server behavior.
- `codes/ui_tars/executor/aillium_core_client.py` marked deprecated as legacy lookup path and kept only for migration reference.

## Remaining valid responsibilities of TARS

1. Contract validation and deterministic dry-run simulation.
2. Audit/evidence payload shaping for transition rollout safety.
3. Temporary MeshCentral remote-handshake bootstrap support.

## Assumptions

- OpenClaw is the strategic runtime/orchestrator owner.
- Aillium Core remains source-of-truth for control-plane concerns.
- MeshCentral repository owns remote-support substrate evolution.
- Existing integrators still need dry-run and handshake compatibility during migration.
