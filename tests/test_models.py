from unittest.mock import patch

import pytest
from pydantic_core._pydantic_core import ValidationError

from statikk.models import (
    DatabaseModel,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
)


def test_index_shorthand():
    class Foo(DatabaseModel):
        player_id: IndexSecondaryKeyField

        @classmethod
        def type_is_primary_key(cls):
            return True

    with patch("statikk.models.uuid.uuid4", return_value="123"):
        foo = Foo(player_id="abc")
        assert foo.model_type() == "Foo"
        assert foo.id == "123"
        assert foo.player_id.value == "abc"
        assert foo.player_id.index_names == ["main-index"]


def test_index_configuration():
    class FooNotMainIndex(DatabaseModel):
        type: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["not-main-index"])

    foo = FooNotMainIndex(type="FooNotMainIndex")
    assert foo.type.value == "FooNotMainIndex"
    assert foo.type.index_names == ["not-main-index"]


def test_invalid_order_config_on_indexes():
    class InvalidOrderingModel(DatabaseModel):
        player_id: IndexPrimaryKeyField
        tier: IndexSecondaryKeyField = IndexSecondaryKeyField(order=1)
        unit_class: IndexSecondaryKeyField

    with pytest.raises(ValidationError):
        InvalidOrderingModel(player_id="abc", unit_class="foo")


def test_all_orders_are_configued_on_indexes():
    class AllOrdersConfiguredModel(DatabaseModel):
        player_id: IndexPrimaryKeyField
        tier: IndexSecondaryKeyField = IndexSecondaryKeyField(order=1)
        unit_class: IndexSecondaryKeyField = IndexSecondaryKeyField(order=2)

    model = AllOrdersConfiguredModel(player_id="abc", unit_class="foo", tier="bar")
    assert model.tier.order == 1
    assert model.unit_class.order == 2


def test_same_order_defined_on_fields():
    class SameOrderDefinedOnFields(DatabaseModel):
        player_id: IndexPrimaryKeyField
        tier: IndexSecondaryKeyField = IndexSecondaryKeyField(order=1)
        unit_class: IndexSecondaryKeyField = IndexSecondaryKeyField(order=1)

    with pytest.raises(ValidationError):
        SameOrderDefinedOnFields(player_id="abc", unit_class="foo", tier="bar")
