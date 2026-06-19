"""Schema compatibility gate. Allow additive nullable; block rename / type change."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import StructType


@dataclass(frozen=True)
class Compat:
    compatible: bool
    added: list[str]
    removed: list[str]
    retyped: list[str]

    @property
    def safe_to_merge(self) -> bool:
        # additive-only evolution is safe; removals/retypes need a contract bump
        return self.compatible and not self.removed and not self.retyped


def check(expected: StructType, incoming: StructType) -> Compat:
    exp = {f.name: f.dataType.simpleString() for f in expected.fields}
    inc = {f.name: f.dataType.simpleString() for f in incoming.fields}
    added = [k for k in inc if k not in exp]
    removed = [k for k in exp if k not in inc]
    retyped = [k for k in exp if k in inc and exp[k] != inc[k]]
    return Compat(
        compatible=not (removed or retyped),
        added=added,
        removed=removed,
        retyped=retyped,
    )
