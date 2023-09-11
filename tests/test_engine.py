from datetime import datetime

from src.statikk.engine import SingleTableApplication
from src.statikk.models import (
    DatabaseModel,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
    Table,
    KeySchema,
    GSI,
)
from moto import mock_dynamodb
import boto3
from botocore.config import Config


class MyAwesomeModel(DatabaseModel):
    player_id: IndexPrimaryKeyField[str]
    type: IndexSecondaryKeyField[str]
    tier: IndexSecondaryKeyField[str]


class DoubleIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField[str]
    type: IndexSecondaryKeyField[str]
    tier: IndexSecondaryKeyField[str]
    card_template_id: IndexPrimaryKeyField[str] = IndexPrimaryKeyField(
        index_name="secondary-index"
    )
    added_at: IndexSecondaryKeyField[datetime] = IndexSecondaryKeyField(
        index_name="secondary-index"
    )


def _dynamo_client():
    return boto3.resource("dynamodb", config=Config(region_name="eu-west-1"))


def _create_dynamodb_table(dynamo, table):
    dynamo.create_table(
        TableName=table.name,
        KeySchema=[{"AttributeName": table.key_schema.hash_key, "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": table.key_schema.hash_key, "AttributeType": "S"},
            {"AttributeName": table.indexes[0].hash_key, "AttributeType": "S"},
            {"AttributeName": table.indexes[0].sort_key, "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": table.indexes[0].name,
                "KeySchema": [
                    {"AttributeName": table.indexes[0].hash_key, "KeyType": "HASH"},
                    {"AttributeName": table.indexes[0].sort_key, "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def test_create_my_awesome_model():
    mock_dynamodb().start()
    my_table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[GSI(name="main-index", hash_key="gsi_sk", sort_key="gsi_pk")],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, my_table)
    dynamo_table = dynamo.Table(my_table.name)
    model = MyAwesomeModel(
        id="foo", player_id="123", type="MyAwesomeModel", tier="LEGENDARY"
    )
    app = SingleTableApplication(table=my_table)
    app.put_item(model)
    assert dynamo_table.get_item(Key={"id": model.id})["Item"] == {
        "id": "foo",
        "player_id": "123",
        "tier": "LEGENDARY",
        "type": "MyAwesomeModel",
        "gsi_sk": "123",
        "gsi_pk": "MyAwesomeModel|LEGENDARY",
    }
    model_2 = MyAwesomeModel(
        id="foo-2", player_id="123", type="MyAwesomeModel", tier="EPIC"
    )
    app.put_item(model_2)
    assert dynamo_table.get_item(Key={"id": model_2.id})["Item"] == {
        "id": "foo-2",
        "player_id": "123",
        "tier": "EPIC",
        "type": "MyAwesomeModel",
        "gsi_sk": "123",
        "gsi_pk": "MyAwesomeModel|EPIC",
    }
    mock_dynamodb().stop()


def test_multi_index_table():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(name="main-index", hash_key="gsi_sk", sort_key="gsi_pk"),
            GSI(name="secondary-index", hash_key="gsi_sk_2", sort_key="gsi_pk_2"),
        ],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, table)
    dynamo_table = dynamo.Table(table.name)
    my_model = DoubleIndexModel(
        id="foo",
        player_id="123",
        type="DoubleIndexModel",
        tier="LEGENDARY",
        card_template_id="abc",
        added_at="2023-09-10 12:00:00",
    )
    app = SingleTableApplication(table=table)
    app.put_item(my_model)
    assert dynamo_table.get_item(Key={"id": my_model.id})["Item"] == {
        "id": "foo",
        "type": "DoubleIndexModel",
        "player_id": "123",
        "tier": "LEGENDARY",
        "card_template_id": "abc",
        "added_at": "2023-09-10 12:00:00",
        "gsi_sk": "123",
        "gsi_pk": "DoubleIndexModel|LEGENDARY",
        "gsi_sk_2": "abc",
        "gsi_pk_2": "2023-09-10 12:00:00",
    }
    mock_dynamodb().stop()
