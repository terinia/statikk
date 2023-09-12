from datetime import datetime

from statikk.conditions import Equals, BeginsWith
from statikk.engine import SingleTableApplication
from statikk.models import (
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
from boto3.dynamodb.conditions import Attr


class MyAwesomeModel(DatabaseModel):
    player_id: IndexPrimaryKeyField[str]
    type: IndexSecondaryKeyField[str]
    tier: IndexSecondaryKeyField[str]


class DoubleIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField[str]
    type: IndexSecondaryKeyField[str]
    tier: IndexSecondaryKeyField[str]
    card_template_id: IndexPrimaryKeyField[str] = IndexPrimaryKeyField(index_names=["secondary-index"])
    added_at: IndexSecondaryKeyField[datetime] = IndexSecondaryKeyField(index_names=["secondary-index"])


class MultiIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField[str]
    card_template_id: IndexPrimaryKeyField[str] = IndexPrimaryKeyField(index_names=["secondary-index"])
    type: IndexSecondaryKeyField[str] = IndexSecondaryKeyField(index_names=["main-index", "secondary-index"])


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
        indexes=[GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk")],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, my_table)
    dynamo_table = dynamo.Table(my_table.name)
    model = MyAwesomeModel(id="foo", player_id="123", type="MyAwesomeModel", tier="LEGENDARY")
    app = SingleTableApplication(table=my_table, models=[MyAwesomeModel])
    app.put_item(model)
    assert dynamo_table.get_item(Key={"id": model.id})["Item"] == {
        "id": "foo",
        "player_id": "123",
        "tier": "LEGENDARY",
        "type": "MyAwesomeModel",
        "gsi_pk": "123",
        "gsi_sk": "MyAwesomeModel|LEGENDARY",
    }
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", type="MyAwesomeModel", tier="EPIC")
    app.put_item(model_2)
    assert dynamo_table.get_item(Key={"id": model_2.id})["Item"] == {
        "id": "foo-2",
        "player_id": "123",
        "tier": "EPIC",
        "type": "MyAwesomeModel",
        "gsi_pk": "123",
        "gsi_sk": "MyAwesomeModel|EPIC",
    }
    mock_dynamodb().stop()


def test_multi_index_table():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk"),
            GSI(name="secondary-index", hash_key="gsi_pk_2", sort_key="gsi_sk_2"),
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
    app = SingleTableApplication(table=table, models=[DoubleIndexModel])
    app.put_item(my_model)
    assert dynamo_table.get_item(Key={"id": my_model.id})["Item"] == {
        "id": "foo",
        "type": "DoubleIndexModel",
        "player_id": "123",
        "tier": "LEGENDARY",
        "card_template_id": "abc",
        "added_at": "2023-09-10 12:00:00",
        "gsi_pk": "123",
        "gsi_sk": "DoubleIndexModel|LEGENDARY",
        "gsi_pk_2": "abc",
        "gsi_sk_2": "2023-09-10 12:00:00",
    }
    mock_dynamodb().stop()


def test_multi_field_index():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk"),
            GSI(name="secondary-index", hash_key="gsi_pk_2", sort_key="gsi_sk_2"),
        ],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, table)
    dynamo_table = dynamo.Table(table.name)
    model = MultiIndexModel(id="card-id", player_id="123", card_template_id="abc", type="LEGENDARY")
    app = SingleTableApplication(table=table, models=[MultiIndexModel])
    app.put_item(model)
    assert dynamo_table.get_item(Key={"id": model.id})["Item"] == {
        "card_template_id": "abc",
        "gsi_pk": "123",
        "gsi_pk_2": "abc",
        "gsi_sk": "LEGENDARY",
        "gsi_sk_2": "LEGENDARY",
        "id": "card-id",
        "player_id": "123",
        "type": "LEGENDARY",
    }
    mock_dynamodb().stop()


def test_integration_get_item():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk"),
            GSI(name="secondary-index", hash_key="gsi_pk_2", sort_key="gsi_sk_2"),
        ],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, table)
    model = MultiIndexModel(id="card-id", player_id="123", card_template_id="abc", type="LEGENDARY")
    app = SingleTableApplication(table=table, models=[MultiIndexModel])
    app.put_item(model)
    item = app.get_item("card-id", MultiIndexModel)
    assert item.id == model.id
    assert item.player_id == model.player_id
    assert item.card_template_id == model.card_template_id
    assert item.type == model.type
    assert item.gsi_pk == "123"
    assert item.gsi_pk_2 == "abc"
    assert item.gsi_sk == "LEGENDARY"
    assert item.gsi_sk_2 == "LEGENDARY"
    mock_dynamodb().stop()


def test_query_model_index():
    mock_dynamodb().start()
    my_table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk")],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, my_table)
    model = MyAwesomeModel(id="foo", player_id="123", type="MyAwesomeModel", tier="LEGENDARY")
    app = SingleTableApplication(table=my_table, models=[MyAwesomeModel])
    app.put_item(model)
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", type="MyAwesomeModel", tier="EPIC")
    app.put_item(model_2)
    models = app.query_index(
        index_name="main-index",
        hash_key=Equals("123"),
        range_key=BeginsWith("MyAwesomeModel"),
        filter_condition=Attr("tier").eq("LEGENDARY"),
        model_class=MyAwesomeModel,
    )
    assert len(models) == 1
    assert models[0].id == model.id
    assert models[0].type == model.type
    assert models[0].tier == model.tier
    mock_dynamodb().stop()


def test_batch_get_items():
    mock_dynamodb().start()
    my_table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk")],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, my_table)
    app = SingleTableApplication(table=my_table, models=[MyAwesomeModel])
    model = MyAwesomeModel(id="foo", player_id="123", type="MyAwesomeModel", tier="LEGENDARY")
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", type="MyAwesomeModel", tier="LEGENDARY")
    app.put_item(model)
    app.put_item(model_2)
    models = app.batch_get_items(["foo", "foo-2"], MyAwesomeModel, batch_size=1)
    assert len(models) == 2
    assert models[0].id == model.id
    assert models[0].type == model.type
    assert models[0].tier == model.tier
    assert models[1].id == model_2.id
    assert models[1].type == model_2.type
    assert models[1].tier == model_2.tier
    mock_dynamodb().stop()


def test_batch_write():
    mock_dynamodb().start()
    my_table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[GSI(name="main-index", hash_key="gsi_pk", sort_key="gsi_sk")],
    )
    dynamo = _dynamo_client()
    _create_dynamodb_table(dynamo, my_table)
    dynamo.Table(my_table.name)
    app = SingleTableApplication(table=my_table, models=[MyAwesomeModel])

    with app.batch_write() as batch:
        for i in range(30):
            model = MyAwesomeModel(id=f"foo_{i}", player_id="123", type="MyAwesomeModel", tier="LEGENDARY")
            batch.put(model)

    models = app.query_index(
        index_name="main-index",
        hash_key=Equals("123"),
        range_key=BeginsWith("MyAwesomeModel"),
        filter_condition=Attr("tier").eq("LEGENDARY"),
        model_class=MyAwesomeModel,
    )
    assert len(models) == 30

    with app.batch_write() as batch:
        for model in models:
            batch.delete(model)

    models = app.query_index(
        index_name="main-index",
        hash_key=Equals("123"),
        range_key=BeginsWith("MyAwesomeModel"),
        filter_condition=Attr("tier").eq("LEGENDARY"),
        model_class=MyAwesomeModel,
    )
    assert len(models) == 0

    mock_dynamodb().stop()
