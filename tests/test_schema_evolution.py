from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from sg_payments.schema_evolution import check


def _s(fields):
    return StructType([StructField(n, t, True) for n, t in fields])


def test_additive_is_safe():
    exp = _s([("id", StringType())])
    inc = _s([("id", StringType()), ("country", StringType())])
    c = check(exp, inc)
    assert c.safe_to_merge and c.added == ["country"]


def test_retype_blocked():
    c = check(_s([("price", StringType())]), _s([("price", LongType())]))
    assert not c.safe_to_merge and c.retyped == ["price"]


def test_removal_blocked():
    c = check(_s([("id", StringType()), ("game", StringType())]), _s([("id", StringType())]))
    assert not c.safe_to_merge and c.removed == ["game"]
