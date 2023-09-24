from datetime import datetime, timezone
from typing import List

import pytest
from boto3.dynamodb.conditions import Attr
from moto import mock_dynamodb

from statikk.conditions import Equals, BeginsWith
from statikk.engine import (
    Table,
    InvalidIndexNameError,
    IncorrectSortKeyError,
    ItemNotFoundError,
)
from statikk.models import (
    DatabaseModel,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
    KeySchema,
    GSI,
    Key,
)


class MyAwesomeModel(DatabaseModel):
    player_id: IndexPrimaryKeyField
    tier: IndexSecondaryKeyField
    name: str = "Foo"
    values: set = {1, 2, 3, 4}
    cost: int = 4


class SimpleModel(DatabaseModel):
    player_id: IndexPrimaryKeyField
    board_id: IndexSecondaryKeyField


class DoubleIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField
    tier: IndexSecondaryKeyField
    card_template_id: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["secondary-index"])
    added_at: IndexSecondaryKeyField = IndexSecondaryKeyField(index_names=["secondary-index"])


class MultiIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField

    card_template_id: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["secondary-index"])
    tier: IndexSecondaryKeyField = IndexSecondaryKeyField(index_names=["secondary-index", "main-index"])
    values: List[int] = [1, 2, 3, 4]

    def include_type_in_sort_key(cls):
        return False


class SomeOtherIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["my-awesome-index"])
    tier: IndexSecondaryKeyField = IndexSecondaryKeyField(index_names=["my-awesome-index"])


def _create_dynamodb_table(table):
    table.create(aws_region="eu-west-1")


def test_create_my_awesome_model():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY")
    table.put_item(model)
    assert table.get_item(id=model.id, model_class=MyAwesomeModel).model_dump() == {
        "id": "foo",
        "player_id": "123",
        "tier": "LEGENDARY",
        "gsi_pk": "123",
        "gsi_sk": "MyAwesomeModel|LEGENDARY",
        "name": "Foo",
        "values": {1, 2, 3, 4},
        "cost": 4,
    }
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="EPIC", name="FooFoo")
    table.put_item(model_2)
    assert table.get_item(id=model_2.id, model_class=MyAwesomeModel).model_dump() == {
        "id": "foo-2",
        "player_id": "123",
        "tier": "EPIC",
        "gsi_pk": "123",
        "gsi_sk": "MyAwesomeModel|EPIC",
        "name": "FooFoo",
        "values": {1, 2, 3, 4},
        "cost": 4,
    }
    mock_dynamodb().stop()


def test_multi_index_table():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            ),
            GSI(
                name="secondary-index",
                hash_key=Key(name="gsi_pk_2"),
                sort_key=Key(name="gsi_sk_2", type=datetime),
            ),
        ],
        models=[DoubleIndexModel],
    )
    _create_dynamodb_table(table)
    my_model = DoubleIndexModel(
        id="foo",
        player_id="123",
        tier="LEGENDARY",
        card_template_id="abc",
        added_at=datetime(2023, 9, 10, 12, 0, 0),
    )
    table.put_item(my_model)
    assert table.get_item(id=my_model.id, model_class=DoubleIndexModel).model_dump() == {
        "id": "foo",
        "player_id": "123",
        "tier": "LEGENDARY",
        "card_template_id": "abc",
        "added_at": 1694340000,
        "gsi_pk": "123",
        "gsi_sk": "DoubleIndexModel|LEGENDARY",
        "gsi_pk_2": "abc",
        "gsi_sk_2": datetime(2023, 9, 10, 10, 0, tzinfo=timezone.utc),
    }
    mock_dynamodb().stop()


def test_incorrect_index_type():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            ),
            GSI(
                name="secondary-index",
                hash_key=Key(name="gsi_pk_2"),
                sort_key=Key(name="gsi_sk_2", type=datetime),
            ),
        ],
        models=[DoubleIndexModel],
    )
    _create_dynamodb_table(table)
    my_model = DoubleIndexModel(
        id="foo",
        player_id="123",
        tier="LEGENDARY",
        card_template_id="abc",
        added_at="2023-01-01 12:00:00",
    )

    with pytest.raises(IncorrectSortKeyError) as e:
        table.put_item(my_model)
    assert (
        e.value.args[0]
        == f"Incorrect sort key type. Sort key type for sort key 'gsi_sk_2' should be: <class 'datetime.datetime'> but got: <class 'str'>"
    )


def test_multi_field_index():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            ),
            GSI(
                name="secondary-index",
                hash_key=Key(name="gsi_pk_2"),
                sort_key=Key(name="gsi_sk_2"),
            ),
        ],
        models=[MultiIndexModel],
    )
    _create_dynamodb_table(table)
    model = MultiIndexModel(id="card-id", player_id="123", card_template_id="abc", tier="LEGENDARY")
    table.put_item(model)
    assert table.get_item(id=model.id, model_class=MultiIndexModel).model_dump() == {
        "card_template_id": "abc",
        "gsi_pk": "123",
        "gsi_pk_2": "abc",
        "gsi_sk": "LEGENDARY",
        "gsi_sk_2": "LEGENDARY",
        "id": "card-id",
        "player_id": "123",
        "tier": "LEGENDARY",
        "values": [1, 2, 3, 4],
    }
    mock_dynamodb().stop()


def test_integration_get_item():
    mock_dynamodb().start()
    table = Table(
        name="my-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            ),
            GSI(
                name="secondary-index",
                hash_key=Key(name="gsi_pk_2"),
                sort_key=Key(name="gsi_sk_2"),
            ),
        ],
        models=[MultiIndexModel],
    )
    _create_dynamodb_table(table)
    model = MultiIndexModel(id="card-id", player_id="123", card_template_id="abc", tier="LEGENDARY")
    table.put_item(model)
    item = table.get_item("card-id", MultiIndexModel)
    assert item.id == model.id
    assert item.player_id == model.player_id
    assert item.card_template_id == model.card_template_id
    assert item.tier == model.tier
    assert item.gsi_pk == "123"
    assert item.gsi_pk_2 == "abc"
    assert item.gsi_sk == "LEGENDARY"
    assert item.gsi_sk_2 == "LEGENDARY"
    mock_dynamodb().stop()


def test_query_model_index():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", name="Terror From Below")
    table.put_item(model)
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="EPIC")
    table.put_item(model_2)
    models = list(
        table.query_index(
            hash_key=Equals("123"),
            range_key=BeginsWith("LEG"),
            filter_condition=Attr("name").eq("Terror From Below"),
            model_class=MyAwesomeModel,
        )
    )
    assert len(models) == 1
    assert models[0].id == model.id
    assert models[0].type == model.type
    assert models[0].tier == model.tier
    mock_dynamodb().stop()


def test_query_index_name_is_provided():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="my-awesome-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[SomeOtherIndexModel],
    )
    _create_dynamodb_table(table)
    model = SomeOtherIndexModel(id="foo", player_id="123", tier="LEGENDARY")
    table.put_item(model)
    model_2 = SomeOtherIndexModel(id="foo-2", player_id="123", tier="EPIC")
    table.put_item(model_2)
    models = list(
        table.query_index(
            index_name="my-awesome-index",
            hash_key=Equals("123"),
            range_key=BeginsWith("SomeOtherIndexModel"),
            filter_condition=Attr("tier").eq("LEGENDARY"),
            model_class=SomeOtherIndexModel,
        )
    )
    assert len(models) == 1
    assert models[0].id == model.id
    assert models[0].type == model.type
    assert models[0].tier == model.tier
    mock_dynamodb().stop()


def test_batch_get_items():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY")
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="LEGENDARY")
    table.put_item(model)
    table.put_item(model_2)
    models = table.batch_get_items(["foo", "foo-2"], MyAwesomeModel, batch_size=1)
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
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)

    with table.batch_write() as batch:
        for i in range(30):
            model = MyAwesomeModel(id=f"foo_{i}", player_id="123", tier="LEGENDARY")
            batch.put(model)

    models = list(
        table.query_index(
            index_name="main-index",
            hash_key=Equals("123"),
            range_key=BeginsWith("MyAwesomeModel"),
            filter_condition=Attr("tier").eq("LEGENDARY"),
            model_class=MyAwesomeModel,
        )
    )
    assert len(models) == 30

    with table.batch_write() as batch:
        for model in models:
            batch.delete(model)

    models = list(
        table.query_index(
            index_name="main-index",
            hash_key=Equals("123"),
            range_key=BeginsWith("MyAwesomeModel"),
            filter_condition=Attr("tier").eq("LEGENDARY"),
            model_class=MyAwesomeModel,
        )
    )
    assert len(models) == 0

    mock_dynamodb().stop()


def test_query_index_does_not_exist():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    with pytest.raises(InvalidIndexNameError) as e:
        list(
            table.query_index(
                hash_key=Equals("123"),
                range_key=BeginsWith("foo"),
                index_name="does-not-exist",
                model_class=MyAwesomeModel,
            )
        )
    assert e.value.args[0] == "The provided index name 'does-not-exist' is not configured on the table."
    mock_dynamodb().stop()


def test_table_delegates():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY").save()
    saved_model = MyAwesomeModel.get(model.id)
    assert model == saved_model
    models = list(MyAwesomeModel.query(hash_key=Equals("123")))
    assert len(models) == 1
    assert models[0] == model
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="bar")
    model_3 = MyAwesomeModel(id="foo-3", player_id="123", tier="bar")
    with MyAwesomeModel.batch_write() as batch:
        batch.put(model_2)
        batch.put(model_3)

    saved_models = MyAwesomeModel.batch_get(
        ["foo", "foo-2", "foo-3"],
    )
    assert len(saved_models) == 3
    assert saved_models[0] == model
    assert saved_models[1] == model_2
    assert saved_models[2] == model_3


def test_exclude_type_from_sort_key():
    class ExcludeTypeModel(DatabaseModel):
        player_id: IndexPrimaryKeyField
        tier: IndexSecondaryKeyField

        @classmethod
        def include_type_in_sort_key(cls):
            return False

    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[ExcludeTypeModel],
    )
    _create_dynamodb_table(table)
    model = ExcludeTypeModel(id="foo", player_id="123", tier="LEGENDARY").save()
    saved_model = ExcludeTypeModel.get(model.id)
    assert "ExcludeTypeModel" not in saved_model.gsi_sk
    mock_dynamodb().stop()


def test_type_is_primary_key():
    class TypeIsPrimaryKeyModel(DatabaseModel):
        tier: IndexSecondaryKeyField = IndexSecondaryKeyField(index_names=["main-index", "secondary-index"])
        foo: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["secondary-index"])

        @classmethod
        def type_is_primary_key(cls):
            return True

        @classmethod
        def include_type_in_sort_key(cls):
            return False

        @classmethod
        def type(cls):
            return "my-type"

    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            ),
            GSI(
                name="secondary-index",
                hash_key=Key(name="gsi_pk_2"),
                sort_key=Key(name="gsi_sk_2"),
            ),
        ],
        models=[TypeIsPrimaryKeyModel],
    )
    _create_dynamodb_table(table)
    model = TypeIsPrimaryKeyModel(tier="LEGENDARY", foo="Bar").save()
    assert model.gsi_pk == "my-type"
    assert model.gsi_sk == "LEGENDARY"
    assert model.gsi_pk_2 == "Bar"
    assert model.gsi_sk_2 == "LEGENDARY"
    mock_dynamodb().stop()


def test_delete_model():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY")
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="EPIC", name="FooFoo")
    model_3 = MyAwesomeModel(id="foo-3", player_id="123", tier="EPIC", name="FooFooFoo")
    table.put_item(model)
    table.put_item(model_2)
    table.delete_item(model.id)
    model_3.delete()
    assert list(table.query_index("123", MyAwesomeModel)) == [model_2]

    mock_dynamodb().stop()


def test_get_item_does_not_exist():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)

    with pytest.raises(ItemNotFoundError) as e:
        table.get_item("foo", MyAwesomeModel)

    mock_dynamodb().stop()


def test_update_set_attribute():
    pass


def test_update_add_attribute():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", name="FooFoo", values={1, 2, 3, 4})
    model.save()
    MyAwesomeModel.update("foo").set("player_id", "456").delete("values", {1}).remove("name").add("cost", 1).execute()
    item = table.get_item("foo", MyAwesomeModel)
    assert item.player_id.value == "456"
    assert item.values == {2, 3, 4}
    assert item.name == "Foo"  # default value
    assert item.cost == 5
    MyAwesomeModel.update("foo").set("name", "FooFoo").execute()
    item = table.get_item("foo", MyAwesomeModel)
    assert item.name == "FooFoo"
    mock_dynamodb().stop()


def test_scan():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", name="FooFoo", values={1, 2, 3, 4})
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="EPIC", name="BarBar")
    model.save()
    model_2.save()
    items = list(MyAwesomeModel.scan())
    assert len(items) == 2
    mock_dynamodb().stop()


def test_query_no_range_key_provided():
    mock_dynamodb().start()
    table = Table(
        name="my-dynamodb-table",
        key_schema=KeySchema(hash_key="id"),
        indexes=[
            GSI(
                name="main-index",
                hash_key=Key(name="gsi_pk"),
                sort_key=Key(name="gsi_sk"),
            )
        ],
        models=[MyAwesomeModel, SimpleModel],
    )
    _create_dynamodb_table(table)
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", name="FooFoo", values={1, 2, 3, 4})
    model.save()
    model_2 = SimpleModel(
        player_id="123",
        board_id="456",
    )
    model_2.save()

    my_awesome_models = list(MyAwesomeModel.query(hash_key=Equals("123")))
    assert len(my_awesome_models) == 1
    mock_dynamodb().stop()
