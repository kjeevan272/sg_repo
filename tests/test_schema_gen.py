from pyspark.sql.types import DateType, DecimalType, StringType

from sg_payments.contracts import PaymentTxn
from sg_payments.schema_gen import pydantic_to_spark


def test_generates_struct_matching_contract():
    s = pydantic_to_spark(PaymentTxn)
    types = {f.name: type(f.dataType) for f in s.fields}
    assert types["transaction_id"] is StringType
    assert types["payment_date"] is DateType
    assert types["price"] is DecimalType
