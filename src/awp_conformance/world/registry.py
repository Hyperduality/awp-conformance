"""World tests register here, each naming the requirements it provides evidence for."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .context import WorldContext

TestFn = Callable[[WorldContext], Awaitable[None]]
Needs = Callable[[WorldContext], str | None]


@dataclass(frozen=True, slots=True)
class WorldTest:
    name: str
    fn: TestFn
    covers: tuple[str, ...]
    mode: str  # any | lockstep | streaming
    needs: Needs | None
    slow: bool
    timeout_s: float


TESTS: list[WorldTest] = []


def world_test(
    name: str,
    covers: tuple[str, ...] | list[str],
    *,
    mode: str = "any",
    needs: Needs | None = None,
    slow: bool = False,
    timeout_s: float = 60.0,
) -> Callable[[TestFn], TestFn]:
    def register(fn: TestFn) -> TestFn:
        TESTS.append(WorldTest(name, fn, tuple(covers), mode, needs, slow, timeout_s))
        return fn

    return register
