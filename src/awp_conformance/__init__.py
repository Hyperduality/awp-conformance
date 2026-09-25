"""Conformance suite for the Agent World Protocol.

Tests a world at a WebSocket URL, or an agent launched against the suite's harness world, against
the requirement matrix of the specification revision in `SPEC_REVISION`.
"""

from importlib.metadata import PackageNotFoundError, version

SPEC_REVISION = "0.1-draft.9"

try:
    __version__ = version("awp-conformance")
except PackageNotFoundError:  # pragma: no cover - running from a source tree without install
    __version__ = "0.0.0"

__all__ = ["SPEC_REVISION", "__version__"]
