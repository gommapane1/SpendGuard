"""
================================================================================
 checks.py — output validators for SpendGuard's quality circuit-breaker
================================================================================

This is the piece a plain budget cap does NOT have: it decides whether the model
actually produced a *correct* result, so SpendGuard can stop paying an agent that
is failing in a loop -- not just stop it when the money runs out.

Each validator returns (ok: bool, reason: str). A policy is a dict:
    {"type": "json_schema", "schema": {...}}
    {"type": "json"}
    {"type": "regex", "pattern": "^SELECT"}
    {"type": "contains", "substring": "DONE"}
    {"type": "nonempty"}
"""

from __future__ import annotations

import json
import re

try:
    import jsonschema
    _HAS_JSONSCHEMA = True
except Exception:
    _HAS_JSONSCHEMA = False


def _extract_json(text: str) -> str:
    """Isola un blocco JSON plausibile: toglie fence ```json ... ``` e prende
    dal primo { o [ all'ultimo } o ] se il modello ha aggiunto testo attorno."""
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.strip("`").strip()
        if t[:4].lower() == "json":
            t = t[4:].strip()
    # se c'e' testo attorno, prova a ritagliare l'oggetto/array piu' esterno
    starts = [i for i in (t.find("{"), t.find("[")) if i != -1]
    ends = [i for i in (t.rfind("}"), t.rfind("]")) if i != -1]
    if starts and ends:
        s, e = min(starts), max(ends)
        if e > s:
            return t[s:e + 1]
    return t


def check_nonempty(text, **_):
    return (bool(text and text.strip()), "empty output")


def check_contains(text, substring="", **_):
    return (substring in (text or ""), f"missing required substring: {substring!r}")


def check_regex(text, pattern="", **_):
    try:
        return (re.search(pattern, text or "") is not None, f"no match for /{pattern}/")
    except re.error as e:
        return (False, f"bad regex: {e}")


def check_json(text, **_):
    try:
        json.loads(_extract_json(text))
        return (True, "")
    except Exception as e:
        return (False, f"invalid JSON: {e}")


_TYPE_MAP = {"object": dict, "array": list, "string": str,
             "number": (int, float), "integer": int, "boolean": bool}


def _minimal_schema(obj, schema):
    """Fallback se jsonschema non e' installato: controlla type, required e i
    tipi delle proprieta' di primo livello. Sufficiente per i casi comuni."""
    exp = schema.get("type")
    if exp in _TYPE_MAP and not isinstance(obj, _TYPE_MAP[exp]):
        return (False, f"expected {exp}")
    if isinstance(obj, dict):
        for req in schema.get("required", []):
            if req not in obj:
                return (False, f"missing required field: {req}")
        for k, sub in (schema.get("properties") or {}).items():
            st = sub.get("type")
            if k in obj and st in _TYPE_MAP and not isinstance(obj[k], _TYPE_MAP[st]):
                return (False, f"field '{k}': expected {st}")
    return (True, "")


def check_json_schema(text, schema=None, **_):
    try:
        obj = json.loads(_extract_json(text))
    except Exception as e:
        return (False, f"invalid JSON: {e}")
    if not schema:
        return (True, "")
    if _HAS_JSONSCHEMA:
        try:
            jsonschema.validate(obj, schema)
            return (True, "")
        except jsonschema.ValidationError as e:
            return (False, f"schema: {e.message}")
        except Exception as e:
            return (False, f"schema error: {e}")
    return _minimal_schema(obj, schema)


CHECKS = {
    "nonempty": check_nonempty,
    "contains": check_contains,
    "regex": check_regex,
    "json": check_json,
    "json_schema": check_json_schema,
}


def run_check(text: str, policy: dict | None):
    """Applica la policy all'output. policy None o tipo sconosciuto -> ok (non blocca)."""
    if not policy:
        return (True, "")
    fn = CHECKS.get(policy.get("type"))
    if not fn:
        return (True, "")
    params = {k: v for k, v in policy.items() if k != "type"}
    try:
        return fn(text, **params)
    except Exception as e:
        return (False, f"check crashed: {e}")