from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from importlib import resources
from jsonschema import Draft202012Validator


CONTRACT_REQUEST = "executor.request"
CONTRACT_RESPONSE = "executor.response"


@dataclass(frozen=True)
class SchemaSet:
    request_schema: dict[str, Any]
    response_schema: dict[str, Any]


class SchemaLoadError(RuntimeError):
    pass


def _iter_json_files(root: resources.abc.Traversable):
    stack = [root]
    while stack:
        node = stack.pop()
        if node.is_file() and node.name.endswith(".json"):
            yield node
            continue
        if node.is_dir():
            for child in node.iterdir():
                stack.append(child)


def _load_from_package() -> list[tuple[str, dict[str, Any]]]:
    try:
        package_root = resources.files("aillium_schemas")
    except Exception as exc:  # pragma: no cover - exercised in higher-level tests
        raise SchemaLoadError(
            "aillium-schemas package is not importable. Install dependency "
            "aillium-schemas @ git+https://github.com/IntAillium/aillium-schemas.git@v0.1.0"
        ) from exc

    docs: list[tuple[str, dict[str, Any]]] = []
    for json_file in _iter_json_files(package_root):
        try:
            docs.append((str(json_file), json.loads(json_file.read_text(encoding="utf-8"))))
        except Exception:
            continue
    return docs


def _load_from_override_dir(override_dir: Path) -> list[tuple[str, dict[str, Any]]]:
    docs: list[tuple[str, dict[str, Any]]] = []
    for file_path in override_dir.rglob("*.json"):
        docs.append((str(file_path), json.loads(file_path.read_text(encoding="utf-8"))))
    return docs


def _topic_matches(path: str, schema: dict[str, Any], topic: str) -> bool:
    probe = " ".join(
        [
            path,
            str(schema.get("$id", "")),
            str(schema.get("title", "")),
            str(schema.get("description", "")),
            str(schema.get("x-contract", "")),
            str(schema.get("name", "")),
        ]
    ).lower()
    variants = {
        topic.lower(),
        topic.replace(".", "_").lower(),
        topic.replace(".", "-").lower(),
    }
    return any(v in probe for v in variants)


def _pick_single(docs: list[tuple[str, dict[str, Any]]], topic: str) -> dict[str, Any]:
    matches = [(path, schema) for path, schema in docs if _topic_matches(path, schema, topic)]
    if not matches:
        raise SchemaLoadError(f"Could not locate schema for {topic} in aillium-schemas resources")
    if len(matches) > 1:
        # Prefer exact filename match first.
        exact = [
            m
            for m in matches
            if any(
                token in Path(m[0]).name.lower()
                for token in [topic.lower(), topic.replace('.', '_').lower(), topic.replace('.', '-').lower()]
            )
        ]
        if len(exact) == 1:
            return exact[0][1]
        raise SchemaLoadError(
            f"Ambiguous schema for {topic}; matched {[m[0] for m in matches]}"
        )
    return matches[0][1]


def load_schemas() -> SchemaSet:
    override_dir = os.getenv("AILLIUM_SCHEMAS_OVERRIDE_DIR")
    docs = (
        _load_from_override_dir(Path(override_dir))
        if override_dir
        else _load_from_package()
    )
    request_schema = _pick_single(docs, CONTRACT_REQUEST)
    response_schema = _pick_single(docs, CONTRACT_RESPONSE)

    # Ensure schemas are internally valid.
    Draft202012Validator.check_schema(request_schema)
    Draft202012Validator.check_schema(response_schema)

    return SchemaSet(request_schema=request_schema, response_schema=response_schema)
