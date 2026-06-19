"""Data-quality package: declarative expectations + referential integrity + reconciliation."""
from .referential import DQError, assert_referential, assert_silver

__all__ = ["DQError", "assert_referential", "assert_silver"]
