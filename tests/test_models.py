from unittest.mock import patch

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
        assert foo.type() == "Foo"
        assert foo.id == "123"
        assert foo.player_id.value == "abc"
        assert foo.player_id.index_names == ["main-index"]


def test_index_configuration():
    class FooNotMainIndex(DatabaseModel):
        type: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["not-main-index"])

    foo = FooNotMainIndex(type="FooNotMainIndex")
    assert foo.type.value == "FooNotMainIndex"
    assert foo.type.index_names == ["not-main-index"]
