from unittest.mock import patch

from src.statikk.models import DatabaseModel, IndexPrimaryKeyField, IndexSecondaryKeyField


def test_index_shorthand():
    class Foo(DatabaseModel):
        type: IndexPrimaryKeyField[str]
        player_id: IndexSecondaryKeyField[str]

    with patch("src.statikk.models.uuid.uuid4", return_value="123"):
        foo = Foo(player_id="abc")
        assert foo.type.value == "Foo"
        assert foo.type.index_name == "main-index"
        assert foo.id == "123"
        assert foo.player_id.value == "abc"
        assert foo.player_id.index_name == "main-index"


def test_index_configuration():
    class FooNotMainIndex(DatabaseModel):
        type: IndexPrimaryKeyField[str] = IndexPrimaryKeyField(index_name="not-main-index")

    foo = FooNotMainIndex(type="FooNotMainIndex")
    assert foo.type.value == "FooNotMainIndex"
    assert foo.type.index_name == "not-main-index"
