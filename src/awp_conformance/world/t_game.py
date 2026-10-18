"""The game profile's reserved device-input type, `input_chunk`, checked from the manifest."""

from __future__ import annotations

from typing import Any

from ..spec import SCHEMAS, VENDOR_FIELD
from .context import WorldContext
from .registry import world_test

ANNOTATIONS = frozenset(
    {
        "$schema",
        "$id",
        "$anchor",
        "$comment",
        "$defs",
        "title",
        "description",
        "default",
        "examples",
    }
)
DEVICE_FIELDS = frozenset({"mouse_dx", "mouse_dy", "scroll_steps", "sticks", "triggers"})
SLOT_MS = ("properties", "slot_ms")
SLOTS = ("properties", "slots")
SLOT_FIELDS = ("properties", "slots", "items", "properties")
KEY_NAME = ("properties", "slots", "items", "properties", "keys", "items")


class RefError(Exception):
    pass


def _pointer(root: Any, ref: str) -> Any:
    node = root
    for part in ref.removeprefix("#").lstrip("/").split("/") if ref != "#" else []:
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        elif isinstance(node, dict) and part in node:
            node = node[part]
        else:
            raise RefError(f"{ref} does not resolve")
    return node


def resolve(node: Any, root: Any, chain: tuple[str, ...] = ()) -> Any:
    """`node` with every local `$ref` replaced by its target, siblings merged over it."""
    if isinstance(node, list):
        return [resolve(v, root, chain) for v in node]
    if not isinstance(node, dict):
        return node
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#"):
        if ref in chain:
            raise RefError(f"{ref} refers to itself")
        target = resolve(_pointer(root, ref), root, (*chain, ref))
        if not isinstance(target, dict):
            raise RefError(f"{ref} is not a schema")
        siblings = {k: resolve(v, root, chain) for k, v in node.items() if k != "$ref"}
        return {**target, **siblings}
    return {k: resolve(v, root, chain) for k, v in node.items()}


def normalize(node: Any) -> Any:
    """Annotations dropped, and `x-awp-closed` read as `additionalProperties: false`."""
    if isinstance(node, list):
        return [normalize(v) for v in node]
    if not isinstance(node, dict):
        return node
    out = {k: normalize(v) for k, v in node.items() if k not in ANNOTATIONS and k != "x-awp-closed"}
    if node.get("x-awp-closed") is True:
        out["additionalProperties"] = False
    if out.get("additionalProperties") is False and out.get("patternProperties") == {
        VENDOR_FIELD: {}
    }:
        del out["patternProperties"]
    return out


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b, strict=True))
    return type(a) is type(b) and a == b


def _int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _diff(w: Any, n: Any, path: tuple[str, ...], out: list[str]) -> None:
    where = "/" + "/".join(path)
    if not (isinstance(w, dict) and isinstance(n, dict)):
        if not _same(w, n):
            out.append(f"{where} is {w!r}, not {n!r}")
        return
    for k in sorted(set(w) | set(n)):
        sub = (*path, k)
        if k not in n:
            if path == SLOT_MS and k == "const":
                v = w[k]
                if not (_int(v) and n["minimum"] <= v <= n["maximum"]):
                    out.append(
                        f"slot_ms is fixed to {v!r}, not an integer from "
                        f"{n['minimum']} to {n['maximum']}"
                    )
            else:
                out.append(f"/{'/'.join(sub)} is added")
        elif k not in w:
            fixed = path == SLOT_MS and "const" in w and k in ("type", "minimum", "maximum")
            if not (fixed or (path == SLOT_FIELDS and k in DEVICE_FIELDS)):
                out.append(f"/{'/'.join(sub)} is removed")
        elif path == SLOTS and k == "maxItems":
            v, low = w[k], n.get("minItems", 0)
            if not (_int(v) and low <= v <= n[k]):
                out.append(f"slots.maxItems is {v!r}, not an integer from {low} to {n[k]}")
        elif path == KEY_NAME and k == "enum":
            v = w[k]
            names = v if isinstance(v, list) else []
            extra = [x for x in names if x not in n[k]]
            if not isinstance(v, list) or extra or len(set(map(repr, names))) != len(names):
                out.append(
                    f"the key-name enum is not a subset of the normative one: {extra or v!r}"
                )
        else:
            _diff(w[k], n[k], sub, out)


def input_chunk_problems(manifest: dict[str, Any]) -> list[str]:
    """How the `input_chunk` declaration departs from AWP-GAM-001; empty when it does not."""
    decl = next(d for d in manifest.get("action_schemas", []) if d.get("type") == "input_chunk")
    problems = []
    if decl.get("duration") != "extended":
        problems.append(f"duration is {decl.get('duration')!r}, not 'extended'")
    policies = decl.get("preemption")
    if "replace" not in (policies if isinstance(policies, list) else [policies]):
        problems.append(f"preemption {policies!r} does not offer 'replace'")
    if decl.get("concurrency_group") != "input":
        problems.append(f"concurrency_group is {decl.get('concurrency_group')!r}, not 'input'")
    normative = SCHEMAS["profiles/input-chunk"]
    try:
        declared = normalize(resolve(decl.get("params_schema"), manifest))
    except RefError as exc:
        return [*problems, f"params_schema: {exc}"]
    _diff(declared, normalize(resolve(normative, normative)), (), problems)
    return problems


def _needs_input_chunk(ctx: WorldContext) -> str | None:
    return None if "input_chunk" in ctx.decls else "input_chunk not declared"


@world_test("input-chunk", ["AWP-GAM-001"], needs=_needs_input_chunk)
async def input_chunk(ctx: WorldContext) -> None:
    problems = input_chunk_problems(ctx.manifest)
    ctx.check("AWP-GAM-001", not problems, "; ".join(problems) or "input_chunk declaration")
