from _decimal import Decimal
from datetime import datetime, timezone
from typing import List, Optional, Type

import pytest
from boto3.dynamodb.conditions import Attr
from moto import mock_dynamodb
from pydantic import BaseModel

from statikk.conditions import Equals, BeginsWith
from statikk.engine import (
    Table,
    InvalidIndexNameError,
    ItemNotFoundError,
)
from statikk.fields import FIELD_STATIKK_TYPE
from statikk.models import (
    DatabaseModel,
    KeySchema,
    GSI,
    Key,
    IndexFieldConfig,
    TrackingMixin,
)


def _create_default_dynamodb_table(models: list[Type[DatabaseModel]]):
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
        models=models,
    )
    _create_dynamodb_table(table)


class MyAwesomeModel(DatabaseModel):
    player_id: str
    tier: str
    name: str = "Foo"
    values: set[int] = {1, 2, 3, 4}
    cost: int = 4
    probability: float = 0.5
    created_at: Optional[datetime] = None

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {"main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["tier"])}

    @classmethod
    def type(cls) -> str:
        return "MyAwesomeModel"


class SimpleModel(DatabaseModel):
    player_id: str
    board_id: str

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {"main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["board_id"])}

    @classmethod
    def type(cls) -> str:
        return "SimpleModel"


class DoubleIndexModel(DatabaseModel):
    player_id: str
    tier: str
    card_template_id: str
    added_at: datetime

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {
            "main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["tier"]),
            "secondary-index": IndexFieldConfig(pk_fields=["card_template_id"], sk_fields=["added_at"]),
        }

    @classmethod
    def type(cls) -> str:
        return "DoubleIndexModel"


class MultiIndexModel(DatabaseModel):
    player_id: str

    card_template_id: str
    tier: str
    values: List[int] = [1, 2, 3, 4]

    @classmethod
    def type(cls) -> str:
        return "MultiIndexModel"

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {
            "main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["tier"]),
            "secondary-index": IndexFieldConfig(pk_fields=["card_template_id"], sk_fields=["tier"]),
        }


class SomeOtherIndexModel(DatabaseModel):
    player_id: str
    tier: str

    @classmethod
    def type(cls) -> str:
        return "SomeOtherIndexModel"

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {"my-awesome-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["tier"])}


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
        "__statikk_type": "MyAwesomeModel",
        "probability": 0.5,
        "created_at": None,
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
        "__statikk_type": "MyAwesomeModel",
        "probability": 0.5,
        "created_at": None,
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
        "added_at": datetime(2023, 9, 10, 12, 0),
        "gsi_pk": "123",
        "gsi_sk": "DoubleIndexModel|LEGENDARY",
        "gsi_pk_2": "abc",
        "gsi_sk_2": datetime(2023, 9, 10, 12, 0),
        "__statikk_type": "DoubleIndexModel",
    }
    mock_dynamodb().stop()


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
        "gsi_sk": "MultiIndexModel|LEGENDARY",
        "gsi_sk_2": "MultiIndexModel|LEGENDARY",
        "id": "card-id",
        "player_id": "123",
        "tier": "LEGENDARY",
        "values": [1, 2, 3, 4],
        "__statikk_type": "MultiIndexModel",
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
    assert item.gsi_sk == "MultiIndexModel|LEGENDARY"
    assert item.gsi_sk_2 == "MultiIndexModel|LEGENDARY"
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
    models: list[MyAwesomeModel] = list(
        table.query_index(
            hash_key=Equals("123"),
            range_key=BeginsWith("LEG"),
            filter_condition=Attr("name").eq("Terror From Below"),
            model_class=MyAwesomeModel,
        )
    )
    assert len(models) == 1
    assert models[0].id == model.id
    assert models[0].type() == model.type()
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
    assert models[0].type() == model.type()
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
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", created_at=datetime(2024, 7, 9))
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="LEGENDARY")
    table.put_item(model)
    table.put_item(model_2)
    models = table.batch_get_items(["foo", "foo-2"], MyAwesomeModel, batch_size=1)
    assert len(models) == 2
    assert models[0].id == model.id
    assert models[0].type() == model.type()
    assert models[0].tier == model.tier
    assert models[0].created_at == datetime(2024, 7, 9)
    assert models[1].id == model_2.id
    assert models[1].type() == model_2.type()
    assert models[1].tier == model_2.tier
    assert models[1].created_at is None
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
    MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY").save()
    saved_model = MyAwesomeModel.get("foo")
    models = list(MyAwesomeModel.query(hash_key=Equals("123")))
    assert len(models) == 1
    assert models[0] == saved_model
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="bar", __statikk_type="MyAwesomeModel")
    model_3 = MyAwesomeModel(id="foo-3", player_id="123", tier="bar", __statikk_type="MyAwesomeModel")
    with MyAwesomeModel.batch_write() as batch:
        batch.put(model_2)
        batch.put(model_3)

    saved_models = MyAwesomeModel.batch_get(
        ["foo", "foo-2", "foo-3"],
    )
    assert len(saved_models) == 3
    assert saved_models[0] == saved_model
    assert saved_models[1] == model_2
    assert saved_models[2] == model_3


def test_type_is_primary_key():
    class TypeIsPrimaryKeyModel(DatabaseModel):
        tier: str
        foo: str

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {
                "main-index": IndexFieldConfig(pk_fields=["__statikk_type"], sk_fields=["tier"]),
                "secondary-index": IndexFieldConfig(pk_fields=["foo"], sk_fields=["tier"]),
            }

        @classmethod
        def type(cls) -> str:
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
    TypeIsPrimaryKeyModel(id="foo", tier="LEGENDARY", foo="Bar").save()
    model = TypeIsPrimaryKeyModel.get("foo")
    assert model.gsi_pk == "my-type"
    assert model.gsi_sk == "LEGENDARY"
    assert model.gsi_pk_2 == "Bar"
    assert model.gsi_sk_2 == "my-type|LEGENDARY"
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
    model = MyAwesomeModel(id="foo", player_id="123", tier="LEGENDARY", __statikk_type="MyAwesomeModel")
    model_2 = MyAwesomeModel(id="foo-2", player_id="123", tier="EPIC", name="FooFoo", __statikk_type="MyAwesomeModel")
    model_3 = MyAwesomeModel(
        id="foo-3", player_id="123", tier="EPIC", name="FooFooFoo", __statikk_type="MyAwesomeModel"
    )
    table.put_item(model)
    table.put_item(model_2)
    table.delete_item(model)
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


def test_update():
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
    (
        model.update()
        .set("player_id", "456")
        .set("tier", "EPIC")
        .delete("values", {1})
        .remove("name")
        .add("cost", 1)
        .execute()
    )
    item = table.get_item("foo", MyAwesomeModel)
    assert item.player_id == "456"
    assert item.values == {2, 3, 4}
    assert item.name == "Foo"  # default value
    assert item.cost == 5
    assert item.tier == "EPIC"
    assert item.gsi_sk == "MyAwesomeModel|EPIC"
    item.update().set("name", "FooFoo").execute()
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


def test_query_hash_key_is_type():
    class Model(DatabaseModel):
        tier: str

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(pk_fields=[FIELD_STATIKK_TYPE], sk_fields=["tier"])}

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
        models=[Model],
    )
    _create_dynamodb_table(table)
    m = Model(tier="LEGENDARY")
    m.save()
    models = list(Model.query(hash_key=Equals("Model")))
    assert len(models) == 1
    assert models[0].tier == "LEGENDARY"
    assert models[0].gsi_pk == "Model"
    assert models[0].gsi_sk == "LEGENDARY"


def test_nested_raw_models():
    class InnerInnerModel(BaseModel, TrackingMixin):
        baz: str

    class InnerModel(BaseModel, TrackingMixin):
        foo: str
        values: List[datetime] = [
            datetime(2023, 9, 9, 12, 0, 0),
            datetime(2023, 9, 9, 13, 0, 0),
        ]
        cost: int = 5
        inner_inner: InnerInnerModel

    class NestedModel(DatabaseModel):
        player_id: str
        unit_class: str
        tier: str
        name: str = "Foo"
        values: set = {1, 2, 3, 4}
        cost: int = 4
        inner_model: InnerModel

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["unit_class", "tier"])}

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
        models=[NestedModel],
    )
    _create_dynamodb_table(table)
    model = NestedModel(
        id="123",
        player_id="456",
        unit_class="Mage",
        tier="EPIC",
        inner_model=InnerModel(foo="bar", inner_inner=InnerInnerModel(baz="baz")),
    )
    model.save()
    item = table.get_item("123", NestedModel)
    assert item.model_dump() == {
        "id": "123",
        "player_id": "456",
        "unit_class": "Mage",
        "tier": "EPIC",
        "name": "Foo",
        "values": {Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4")},
        "cost": 4,
        "inner_model": {
            "foo": "bar",
            "values": [
                datetime(2023, 9, 9, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 9, 9, 11, 0, tzinfo=timezone.utc),
            ],
            "cost": 5,
            "inner_inner": {"baz": "baz"},
        },
        "gsi_pk": "456",
        "gsi_sk": "NestedModel|Mage|EPIC",
        "__statikk_type": "NestedModel",
    }


def test_nested_hierarchies():
    class OptionalModel(DatabaseModel):
        xax: str = "xax"

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["xax"])}

        @classmethod
        def is_nested(cls) -> bool:
            return True

    class TriplyNested(DatabaseModel):
        faz: str = "faz"
        optional: Optional[OptionalModel] = None

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["faz"])}

        @classmethod
        def is_nested(cls) -> bool:
            return True

    class DoublyNestedModel(DatabaseModel):
        bar: str
        items: list[TriplyNested] = []

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["bar"])}

        @classmethod
        def is_nested(cls) -> bool:
            return True

        def should_write_to_database(self) -> bool:
            return self.bar == "bar"

    class NestedModel(DatabaseModel):
        foo: str
        doubly_nested: list[DoublyNestedModel] = []

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["foo"])}

        @classmethod
        def is_nested(cls) -> bool:
            return True

    class ModelHierarchy(DatabaseModel):
        foo_id: str
        state: str
        nested: NestedModel

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(pk_fields=["foo_id"], sk_fields=["state"])}

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
        models=[ModelHierarchy, NestedModel, DoublyNestedModel, TriplyNested, OptionalModel],
    )
    _create_dynamodb_table(table)
    triple_nested_model = TriplyNested(faz="faz", optional=OptionalModel(xax="xax"))
    doubly_nested = DoublyNestedModel(bar="bar", items=[triple_nested_model])
    double_nested_no_write = DoublyNestedModel(bar="far", items=[TriplyNested(faz="faz")])
    nested = NestedModel(foo="foo", doubly_nested=[doubly_nested, double_nested_no_write])
    model_hierarchy = ModelHierarchy(foo_id="foo_id", state="state", nested=nested)
    model_hierarchy.save()
    hierarchy = ModelHierarchy.query_hierarchy(hash_key=Equals("foo_id"))
    assert hierarchy.gsi_pk == "foo_id"
    assert hierarchy.gsi_sk == "ModelHierarchy|state"
    assert hierarchy.nested.gsi_pk == "foo_id"
    assert hierarchy.nested.gsi_sk == "ModelHierarchy|state|NestedModel|foo"
    assert len(hierarchy.nested.doubly_nested) == 1  # doubly_nested_no_write is not saved to the database
    assert hierarchy.nested.doubly_nested[0].gsi_pk == "foo_id"
    assert hierarchy.nested.doubly_nested[0].gsi_sk == "ModelHierarchy|state|NestedModel|foo|DoublyNestedModel|bar"
    assert hierarchy.nested.doubly_nested[0].items[0].gsi_pk == "foo_id"
    assert (
        hierarchy.nested.doubly_nested[0].items[0].gsi_sk
        == "ModelHierarchy|state|NestedModel|foo|DoublyNestedModel|bar|TriplyNested|faz"
    )
    assert hierarchy.nested.doubly_nested[0].items[0].optional.gsi_pk == "foo_id"
    assert (
        hierarchy.nested.doubly_nested[0].items[0].optional.gsi_sk
        == "ModelHierarchy|state|NestedModel|foo|DoublyNestedModel|bar|TriplyNested|faz|OptionalModel|xax"
    )
    hierarchy.nested.doubly_nested[0].items[0].mark_for_delete()
    hierarchy.save()
    hierarchy = ModelHierarchy.query_hierarchy(hash_key=Equals("foo_id"))
    assert len(hierarchy.nested.doubly_nested[0].items) == 0
    hierarchy.delete()
    assert list(table.scan()) == []


def test_rebuild_model_indexes():
    class MyDatabaseModel(DatabaseModel):
        foo: str = "foo"
        bar: str = "bar"

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(pk_fields=["foo"], sk_fields=["bar"])}

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
        models=[MyDatabaseModel],
    )
    _create_dynamodb_table(table)
    my_database_model = MyDatabaseModel(foo="foo", bar="bar")
    my_database_model.build_model_indexes()
    assert my_database_model.gsi_pk == "foo"
    assert my_database_model.gsi_sk == "MyDatabaseModel|bar"


def test_add_child_node():
    class MyOtherNestedDatabaseModel(DatabaseModel):
        baz: str

        @classmethod
        def is_nested(cls) -> bool:
            return True

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["baz"])}

        __hash__ = object.__hash__

    class MyNestedDatabaseModel(DatabaseModel):
        bar: str
        other_nested: Optional[MyOtherNestedDatabaseModel] = None
        list_nested: list[MyOtherNestedDatabaseModel] = []
        other_list_nested: list[MyOtherNestedDatabaseModel] = []

        @classmethod
        def is_nested(cls) -> bool:
            return True

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(sk_fields=["bar"])}

    class MyDatabaseModel(DatabaseModel):
        foo: str
        nested: MyNestedDatabaseModel

        @classmethod
        def index_definitions(cls) -> dict[str, IndexFieldConfig]:
            return {"main-index": IndexFieldConfig(pk_fields=["foo"], sk_fields=[FIELD_STATIKK_TYPE])}

        @property
        def should_track_session(self) -> bool:
            return True

    _create_default_dynamodb_table([MyDatabaseModel, MyNestedDatabaseModel, MyOtherNestedDatabaseModel])
    my_database_model = MyDatabaseModel(foo="foo", nested=MyNestedDatabaseModel(bar="bar"))
    my_database_model.build_model_indexes()
    my_database_model.nested.add_child_node("other_nested", MyOtherNestedDatabaseModel(baz="baz"))
    my_database_model.nested.add_child_node("list_nested", MyOtherNestedDatabaseModel(baz="bazz"))
    my_database_model.nested.add_child_node("other_list_nested", MyOtherNestedDatabaseModel(baz="bazzz"))
    assert my_database_model.nested.other_nested.baz == "baz"
    assert my_database_model.nested.list_nested[0].baz == "bazz"
    other_list_nested = my_database_model.nested.other_list_nested[0]
    assert other_list_nested.baz == "bazzz"
    assert other_list_nested._parent == my_database_model.nested
    assert other_list_nested.gsi_pk == other_list_nested._parent.gsi_pk
    assert other_list_nested.gsi_sk == "MyDatabaseModel|MyNestedDatabaseModel|bar|MyOtherNestedDatabaseModel|bazzz"
    assert my_database_model.nested.other_nested._parent == my_database_model.nested
    assert my_database_model.nested.list_nested[0]._parent == my_database_model.nested
    my_database_model.nested.add_child_node("other_list_nested", my_database_model.nested.list_nested[0])
    assert my_database_model.nested.list_nested[0]._parent_changed is True
    other_list_nested_new = my_database_model.nested.other_list_nested[1]
    assert len(my_database_model.nested.other_list_nested) == 2
    assert other_list_nested_new._parent_changed is False
    assert other_list_nested_new.gsi_sk == "MyDatabaseModel|MyNestedDatabaseModel|bar|MyOtherNestedDatabaseModel|bazz"
    my_database_model.nested.add_child_node("list_nested", other_list_nested_new)
    assert my_database_model.nested.list_nested[0]._parent_changed is False
    assert my_database_model.nested.list_nested[0]._parent == my_database_model.nested
    assert my_database_model.nested.list_nested[0].gsi_pk == "foo"
    assert (
        my_database_model.nested.list_nested[0].gsi_sk
        == "MyDatabaseModel|MyNestedDatabaseModel|bar|MyOtherNestedDatabaseModel|bazz"
    )
    assert other_list_nested_new._parent_changed is True
    my_database_model.save()
    my_database_model = MyDatabaseModel.query_hierarchy(hash_key=Equals("foo"))
    assert len(my_database_model.nested.list_nested) == 1
    assert len(my_database_model.nested.other_list_nested) == 1
    hierarchy = MyDatabaseModel.query_hierarchy(hash_key=Equals("foo"))
    assert hierarchy.is_persisted is True
    assert hierarchy.nested.is_persisted is True
    assert hierarchy.nested.list_nested[0].is_persisted is True
    assert hierarchy.nested.other_list_nested[0].is_persisted is True
    assert hierarchy.nested.other_nested.is_persisted is True
