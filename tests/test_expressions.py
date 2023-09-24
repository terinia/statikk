import boto3
import pytest
from moto import mock_dynamodb

from statikk.expressions import (
    UpdateExpressionBuilder,
)
from statikk.models import DatabaseModel


class MyModel(DatabaseModel):
    attribute1: int
    attribute2: list
    attribute3: str
    attribute4: set
    name: str  # reserved keyword
    value: int  # reserved keyword
    values: set  # reserved keyword


# A fixture to create a mock DynamoDB table
@pytest.fixture
def create_mocked_table():
    with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
        dynamodb.create_table(
            TableName="TestTable",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        yield


def test_set_method(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    builder.set("attribute1", 10).set("name", "Bob")
    expr, values, attribute_names = builder.build()
    assert expr == "SET attribute1 = :attribute1, #n_name = :name"
    assert values == {":attribute1": 10, ":name": "Bob"}
    assert attribute_names == {"#n_name": "name"}


def test_add_method_valid(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    builder.add("value", 5).add("attribute1", 2)
    expr, values, attr_names = builder.build()
    assert expr == "ADD #n_value :value, attribute1 :attribute1"
    assert values == {":value": 5, ":attribute1": 2}
    assert attr_names == {"#n_value": "value"}


def test_add_method_invalid_type(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    with pytest.raises(ValueError, match="The ADD action does not support"):
        builder.add("attribute3", "invalid")


def test_add_method_invalid_nested(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    with pytest.raises(ValueError, match="The ADD action cannot be used on nested attributes."):
        builder.add("attribute.nested", 5)


def test_remove_method(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    builder.remove("attribute3").remove("name")
    expr, _, attr_names = builder.build()
    assert expr == "REMOVE attribute3, #n_name"
    assert attr_names == {"#n_name": "name", "attribute3": "attribute3"}


def test_delete_method_valid(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    builder.delete("attribute4", {"value"}).delete("values", {1})
    expr, values, attr_names = builder.build()
    assert expr == "DELETE attribute4 :attribute4, #n_values :values"
    assert values == {":attribute4": {"value"}, ":values": {1}}
    assert attr_names == {"#n_values": "values", "attribute4": "attribute4"}


def test_delete_method_invalid(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    with pytest.raises(ValueError, match="The DELETE action only supports"):
        builder.delete("attribute1", 5)


def test_multiple_methods(create_mocked_table):
    builder = UpdateExpressionBuilder(MyModel)
    builder.set("attribute1", 10).add("attribute1", 5).remove("attribute3").delete("attribute4", {"value"})
    expr, values, attr_names = builder.build()
    assert (
        expr
        == "SET attribute1 = :attribute1 ADD attribute1 :attribute1 REMOVE attribute3 DELETE attribute4 :attribute4"
    )
    assert values == {":attribute1": 5, ":attribute4": {"value"}}
