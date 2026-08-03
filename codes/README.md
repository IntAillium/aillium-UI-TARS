# ui-tars

A python package for parsing VLM-generated GUI action instructions into executable pyautogui codes.

---

## Introduction

`ui-tars` is a Python package for parsing VLM-generated GUI action instructions, automatically generating pyautogui scripts, and supporting coordinate conversion and smart image resizing.

- Supports multiple VLM output formats (e.g., Qwen-VL, Seed-VL)
- Automatically handles coordinate scaling and format conversion
- One-click generation of pyautogui automation scripts

---

## Quick Start

### Installation

```bash
pip install ui-tars
# or
uv pip install ui-tars
```

### Parse output into structured actions

```python
from ui_tars.action_parser import parse_action_to_structure_output, parsing_response_to_pyautogui_code

response = "Thought: Click the button\nAction: click(point='<point>200 300</point>')"
original_image_width, original_image_height = 1920, 1080
parsed_dict = parse_action_to_structure_output(
    response,
    factor=1000,
    origin_resized_height=original_image_height,
    origin_resized_width=original_image_width,
    model_type="doubao"
)
print(parsed_dict)
parsed_pyautogui_code = parsing_response_to_pyautogui_code(
    responses=parsed_dict,
    image_height=original_image_height,
    image_width=original_image_width
)
print(parsed_pyautogui_code)
```

### Generate pyautogui automation script

```python
from ui_tars.action_parser import parsing_response_to_pyautogui_code

pyautogui_code = parsing_response_to_pyautogui_code(parsed_dict, original_image_height, original_image_width)
print(pyautogui_code)
```

### Visualize coordinates on the image (optional)

```python
from PIL import Image, ImageDraw
import numpy as np
import matplotlib.pyplot as plt

image = Image.open("your_image_path.png")
start_box = parsed_dict[0]["action_inputs"]["start_box"]
coordinates = eval(start_box)
x1 = int(coordinates[0] * original_image_width)
y1 = int(coordinates[1] * original_image_height)
draw = ImageDraw.Draw(image)
radius = 5
draw.ellipse((x1 - radius, y1 - radius, x1 + radius, y1 + radius), fill="red", outline="red")
plt.imshow(np.array(image))
plt.axis("off")
plt.show()
```

---

## API Documentation

### parse_action_to_structure_output

```python
def parse_action_to_structure_output(
    text: str,
    factor: int,
    origin_resized_height: int,
    origin_resized_width: int,
    model_type: str = "qwen25vl",
    max_pixels: int = 16384 * 28 * 28,
    min_pixels: int = 100 * 28 * 28
) -> list[dict]:
    ...
```

**Description:**
Parses output action instructions into structured dictionaries, automatically handling coordinate scaling and box/point format conversion.

**Parameters:**
- `text`: The output string
- `factor`: Scaling factor
- `origin_resized_height`/`origin_resized_width`: Original image height/width
- `model_type`: Model type (e.g., "qwen25vl", "doubao")
- `max_pixels`/`min_pixels`: Image pixel upper/lower limits

**Returns:**
A list of structured actions, each as a dict with fields like `action_type`, `action_inputs`, `thought`, etc.

---

### parsing_response_to_pyautogui_code

```python
def parsing_response_to_pyautogui_code(
    responses: dict | list[dict],
    image_height: int,
    image_width: int,
    input_swap: bool = True
) -> str:
    ...
```

**Description:**
Converts structured actions into a pyautogui script string, supporting click, type, hotkey, drag, scroll, and more.

**Parameters:**
- `responses`: Structured actions (dict or list of dicts)
- `image_height`/`image_width`: Image height/width
- `input_swap`: Whether to use clipboard paste for typing (default True)

**Returns:**
A pyautogui script string, ready for automation execution.

---

## Contribution

Contributions, issues, and suggestions are welcome!

---

## License

Apache-2.0 License

---

## Aillium Executor Dry-Run (A-5a MVP)

This repository now keeps a **reduced transition role**: HTTP dry-run + remote-handshake support for contract validation and deterministic mock execution while OpenClaw owns runtime orchestration.

### Behavior


### Transition scope and deprecations

- Keep: schema-driven validation, deterministic dry-run responses, audit/evidence payload shaping.
- Keep (temporary): remote-handshake bootstrap path for MeshCentral-assisted transition scenarios.
- Deprecated: legacy push/inbound worker paths (`/executor/push`, `/executor/inbound`).
- Deprecated directionally: any duplicated browser/runtime orchestration logic now superseded by OpenClaw.

- Endpoint: `POST /executor/dry-run`
- Validates input against `executor.request` from `aillium-schemas`
- Produces `executor.response` payload validated against `aillium-schemas`
- Dry-run only: no real UI automation, no OS input, no browser sessions, no MeshCentral sessions
- Structured JSON audit logs include correlation IDs where present (`tenantId`, `requestId`, `traceId`)

### Schema source of truth

Schemas are loaded from installed package resources of:

- `aillium-schemas @ git+https://github.com/IntAillium/aillium-schemas.git@v0.1.0`

No schema JSON is vendored in this repository.

### Run

```bash
cd codes
python -m ui_tars.executor.server
```

Optional environment variables:

- `EXECUTOR_HOST` (default `0.0.0.0`)
- `EXECUTOR_PORT` (default `8080`)
- `AILLIUM_SCHEMAS_OVERRIDE_DIR` (test/dev override path; production should use installed package resources)

### Example request

```bash
curl -sS -X POST http://localhost:8080/executor/dry-run \
  -H 'content-type: application/json' \
  -H 'x-trace-id: trace-123' \
  -d '{
    "tenantId": "tenant-001",
    "requestId": "req-001",
    "meta": {"simulate_failure": false}
  }'
```


## Aillium Worker Poll Loop (Milestone A)

Primary runtime is now the polling worker (not inbound push server).

### Start worker against real aillium-core

```bash
cd codes
PYTHONPATH=. MESHCENTRAL_MOCK=1 python -m ui_tars.executor.worker
```

Required env vars:
- `AILLIUM_CORE_BASE_URL`
- `AILLIUM_CORE_TOKEN` (a pre-issued JWT with `principal_type=WORKER`; human
  login tokens and development bypass tokens are rejected)
- `AILLIUM_TENANT_ID` (must match both the token's tenant claim and Core's
  `AILLIUM_WORKER_PRINCIPAL_SCOPES` entry)

Optional env vars:
- `AILLIUM_CORE_TIMEOUT_SECONDS` (default `10`)
- `AILLIUM_POLL_INTERVAL_SECONDS` (default `0.2`)
- `AILLIUM_IDLE_BACKOFF_SECONDS` (default `1.0`)
- `AILLIUM_WORKER_ID` (or auto-generated and persisted)
- `AILLIUM_WORKER_ID_FILE` (default `~/.aillium-worker-id`)
- `AILLIUM_TASK_TYPE` (default `remote-handshake`)
- `AILLIUM_EXECUTOR_TYPE` (default `ui-tars`)
- `AILLIUM_VISIBILITY_TIMEOUT_SECONDS` (default `60`)
- `AILLIUM_LEASE_RENEW_INTERVAL_SECONDS` (default: one third of the visibility
  timeout)

The worker keeps the lease token returned by Core, uses lease-bound payload and
callback endpoints, and renews the lease throughout long-running execution.

### End-to-end local MVP harness

See `docs/milestone_a_local_mvp.md` and run:

```bash
cd codes
PYTHONPATH=. MESHCENTRAL_MOCK=1 python scripts/local_milestone_a_mvp.py
```
