# AI Development Guardrails

This repository is part of the Aillium platform.

Automated agents and AI tools must follow these rules:

- Treat Aillium platform documentation and `aillium-schemas` as the single source of truth
- Do not introduce new architecture patterns without approval
- Do not bypass policy, audit, or approval mechanisms
- Do not add hardcoded credentials
- Do not merge unrelated concerns into this repo
- Follow schemas defined in aillium-schemas
- Prefer clarity over cleverness

Violations of these rules are considered defects.

# Aillium AI Guardrails — UI-TARS (Execution Layer)

This document defines **non-negotiable constraints** for the `aillium-tars` service.

`aillium-tars` is a **deterministic UI execution worker**.  
It is not an AI planner, not a policy engine, and not a system of record.

---

## Role & Scope

`aillium-tars` exists solely to:

- Execute **pre-approved ExecutionPlans** generated upstream
- Perform **UI-driven automation** where no API exists
- Capture **evidence artifacts** (screenshots, logs, recordings)
- Report execution results back to `aillium-core`

It does **not** make decisions.

---

## Explicit Prohibitions

The following are **strictly forbidden** in this repository:

### ❌ Planning or Reasoning
- No task decomposition
- No goal interpretation
- No intent rewriting
- No autonomous decision-making

### ❌ Policy Enforcement
- No risk evaluation
- No approval gating
- No budget enforcement
- No tenant policy interpretation

(All policy decisions happen in `aillium-openclaw` and `aillium-core`.)

### ❌ Direct AI / LLM Usage
- No direct OpenAI / Anthropic / Gemini calls
- No prompt generation
- No free-form model inference

### ❌ System-of-Record State
- No canonical task state
- No lifecycle ownership
- No database treated as authoritative

---

## Required Behavior

### Deterministic Execution
- All actions must be driven by an **ExecutionPlan**
- Execution must be replayable and auditable
- No branching beyond plan instructions

### Approved Inputs Only
- Accepts plans **only from `aillium-core`**
- Rejects unsigned, malformed, or out-of-contract payloads
- Never pulls work directly from OpenClaw
- Enforces tenant-scoped execution boundaries on every request

### Evidence First
- Capture UI evidence wherever possible
- Store artifacts immutably
- Reference artifacts via URIs only

### Stateless Preference
- No persistent local state unless strictly required
- All recovery must be possible via upstream replay

---

## Trust Boundary

`aillium-tars` is a **low-trust execution surface**.

Assume:
- Inputs may be malformed
- UI targets may be hostile
- Execution may be interrupted

Design accordingly.

---

## Enforcement

Violations of these guardrails are considered **critical architecture defects**.

Any change that expands scope **must be approved by the platform owner**.
