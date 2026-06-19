"""Pydantic -> Spark StructType. One source of truth for schema across Python/Spark/Glue."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import get_args, get_origin

from pydantic import BaseModel
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_MAP: dict[type, DataType] = {
    str: StringType(),
    int: LongType(),
    bool: BooleanType(),
    float: DoubleType(),
    date: DateType(),
    datetime: TimestampType(),
    Decimal: DecimalType(18, 4),
}


def _spark_type(py: type) -> DataType:
    if get_origin(py) is not None:  # Optional[X], list[X], etc.
        py = next((a for a in get_args(py) if a is not type(None)), str)
    if isinstance(py, type) and issubclass(py, BaseModel):
        return pydantic_to_spark(py)
    return _MAP.get(py, StringType())


def pydantic_to_spark(model: type[BaseModel]) -> StructType:
    fields = [
        StructField(name, _spark_type(f.annotation), nullable=not f.is_required())
        for name, f in model.model_fields.items()
    ]
    return StructType(fields)
