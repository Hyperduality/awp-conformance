"""Bundle the pinned spec's schemas, lifecycle table, and requirement matrix.

`--check` fails on drift.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "spec"
DEST = ROOT / "src" / "awp_conformance" / "_spec"

# | [AWP-ACT-001](/spec/loop/actions) | MUST | both | all | `core` | AWP-ACT-001 | text |
ROW = re.compile(r"^\| \[(AWP-[A-Z]+-\d{3})\]\(([^)]*)\) \| (MUST|SHOULD|MAY) \|.*\| ([^|]*) \|$")


def requirements() -> list[dict[str, str]]:
    rows = yaml.safe_load((SPEC / "spec" / "requirements.yaml").read_text())["requirements"]
    pages: dict[str, tuple[str, str, str]] = {}
    for line in (SPEC / "spec" / "requirements.mdx").read_text().splitlines():
        m = ROW.match(line)
        if m:
            pages[m.group(1)] = (m.group(3), m.group(2), m.group(4).strip())
    out = []
    for r in rows:
        level, page, text = pages[r["id"]]
        out.append(
            {**{k: str(v) for k, v in r.items()}, "level": level, "page": page, "text": text}
        )
    return out


def expected() -> dict[Path, str]:
    files: dict[Path, str] = {}
    schema_dir = SPEC / "schemas" / "v0.1"
    for src in sorted(schema_dir.rglob("*.schema.json")):
        files[DEST / "schemas" / src.relative_to(schema_dir)] = src.read_text()
    table = yaml.safe_load((SPEC / "spec" / "action-lifecycle.yaml").read_text())
    table.pop("pre_execution_cancel", None)  # YAML anchor holder, not part of the table
    files[DEST / "lifecycle.json"] = json.dumps(table, indent=2) + "\n"
    files[DEST / "requirements.json"] = json.dumps(requirements(), indent=1) + "\n"
    files[DEST / "frames.json"] = (SPEC / "schemas" / "test-vectors" / "frames.json").read_text()
    return files


def main() -> int:
    if not (SPEC / "schemas").is_dir():
        print("spec/ submodule missing; run `git submodule update --init`", file=sys.stderr)
        return 1
    want = expected()
    have = {p: p.read_text() for p in DEST.rglob("*.json")} if DEST.exists() else {}
    if "--check" in sys.argv:
        drift = sorted(
            str(p.relative_to(ROOT))
            for p in want.keys() | have.keys()
            if want.get(p) != have.get(p)
        )
        for name in drift:
            print(f"DRIFT: {name}; run `python scripts/sync_spec.py`", file=sys.stderr)
        return 1 if drift else 0
    for p in have.keys() - want.keys():
        p.unlink()
    for p, text in want.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    print(f"synced {len(want)} files into {DEST.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
