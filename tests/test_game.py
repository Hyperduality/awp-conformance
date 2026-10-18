"""AWP-GAM-001: an input_chunk declaration narrows the normative params schema in four ways only."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from awp_conformance import spec
from awp_conformance.scope import Scope
from awp_conformance.world.t_game import input_chunk_problems

EXAMPLE = Path(__file__).resolve().parent.parent / "spec/examples/v0.1/world-manifest-input.json"
GAMEPAD = ["GamepadSouth", "GamepadEast", "GamepadDpadUp", "GamepadStart"]


def manifest(edit: Callable[[dict[str, Any]], Any] = lambda s: None, **decl: Any) -> dict[str, Any]:
    """A manifest declaring input_chunk with a copy of the normative schema, changed by `edit`."""
    params = copy.deepcopy(spec.SCHEMAS["profiles/input-chunk"])
    for k in ("$schema", "$id", "title", "description"):
        params.pop(k)
    key_name = params.pop("$defs")["key_name"]
    params["properties"]["slots"]["items"]["properties"]["keys"]["items"] = {
        "$ref": "#/$defs/input_key_name"
    }
    m: dict[str, Any] = {
        "action_schemas": [
            {
                "type": "input_chunk",
                "params_schema": {"$ref": "#/$defs/input_chunk_params"},
                "duration": "extended",
                "preemption": ["replace", "queue"],
                "concurrency_group": "input",
                **decl,
            }
        ],
        "$defs": {"input_chunk_params": params, "input_key_name": key_name},
    }
    edit(m["$defs"])
    return m


def slot_fields(defs: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = defs["input_chunk_params"]["properties"]["slots"]["items"][
        "properties"
    ]
    return fields


def set_slot_ms(value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda d: d["input_chunk_params"]["properties"]["slot_ms"].update(const=value)


def set_max_items(value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda d: d["input_chunk_params"]["properties"]["slots"].update(maxItems=value)


def set_keys(names: list[str]) -> Callable[[dict[str, Any]], None]:
    return lambda d: d["input_key_name"].update(enum=names)


def remove(*names: str) -> Callable[[dict[str, Any]], None]:
    def edit(d: dict[str, Any]) -> None:
        for n in names:
            del slot_fields(d)[n]

    return edit


def closed_by_additional_properties(d: dict[str, Any]) -> None:
    for node in (d["input_chunk_params"], *slot_fields(d).values()):
        if node.pop("x-awp-closed", False):
            node["additionalProperties"] = False


def test_the_normative_schema_itself_conforms():
    assert input_chunk_problems(manifest()) == []


def test_the_specification_example_conforms():
    example = json.loads(EXAMPLE.read_text())
    assert input_chunk_problems(example) == []


@pytest.mark.parametrize(
    "edit",
    [
        pytest.param(set_slot_ms(33), id="slot_ms-const"),
        pytest.param(set_max_items(16), id="maxItems-lowered"),
        pytest.param(set_keys(GAMEPAD), id="keys-subset"),
        pytest.param(remove("mouse_dx", "mouse_dy", "scroll_steps"), id="device-fields-removed"),
        pytest.param(closed_by_additional_properties, id="closed-by-additionalProperties"),
    ],
)
def test_a_permitted_narrowing_conforms(edit):
    assert input_chunk_problems(manifest(edit)) == []


def test_slot_ms_may_be_fixed_by_const_alone():
    def edit(d: dict[str, Any]) -> None:
        d["input_chunk_params"]["properties"]["slot_ms"] = {"const": 50}

    assert input_chunk_problems(manifest(edit)) == []


@pytest.mark.parametrize(
    ("edit", "says"),
    [
        pytest.param(
            set_slot_ms(2000), "slot_ms is fixed to 2000", id="slot_ms-const-out-of-range"
        ),
        pytest.param(
            lambda d: d["input_chunk_params"]["properties"]["slot_ms"].update(maximum=100),
            "/properties/slot_ms/maximum",
            id="slot_ms-range-changed",
        ),
        pytest.param(set_max_items(512), "slots.maxItems is 512", id="maxItems-raised"),
        pytest.param(
            set_keys([*GAMEPAD, "GamepadLeftTrigger"]), "GamepadLeftTrigger", id="keys-added"
        ),
        pytest.param(remove("keys"), "/keys is removed", id="keys-removed"),
        pytest.param(
            lambda d: slot_fields(d)["sticks"]["properties"].pop("right_x"),
            "right_x is removed",
            id="stick-axis-removed",
        ),
        pytest.param(
            lambda d: slot_fields(d).update(gyro={"type": "number"}),
            "gyro is added",
            id="device-field-added",
        ),
    ],
)
def test_any_other_change_fails(edit, says):
    problems = input_chunk_problems(manifest(edit))
    assert any(says in p for p in problems), problems


@pytest.mark.parametrize(
    ("decl", "says"),
    [
        ({"duration": "instant"}, "duration"),
        ({"preemption": "queue"}, "'replace'"),
        ({"concurrency_group": "keyboard"}, "concurrency_group"),
        ({"params_schema": {"$ref": "#/$defs/missing"}}, "does not resolve"),
    ],
)
def test_the_declaration_fields_are_checked(decl, says):
    problems = input_chunk_problems(manifest(**decl))
    assert any(says in p for p in problems), problems


def test_the_gate_follows_the_declaration():
    req = spec.REQUIREMENTS["AWP-GAM-001"]
    with_chunk = Scope("world", {"streaming"}, manifest())
    without = Scope("world", {"streaming"}, {"action_schemas": [{"type": "move"}]})
    assert with_chunk.applicable(req)[0]
    assert not without.applicable(req)[0]
