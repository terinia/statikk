from statikk.models import DatabaseModel


class MyDoublyNestedDatabaseModel(DatabaseModel):
    baz: str

    @classmethod
    def is_nested(cls) -> bool:
        return True


class MyNestedDatabaseModel(DatabaseModel):
    bar: str
    doubly_nested: MyDoublyNestedDatabaseModel

    @classmethod
    def is_nested(cls) -> bool:
        return True


class MyDatabaseModel(DatabaseModel):
    foo: str
    nested: MyNestedDatabaseModel


class MyDatabaseModelWithList(DatabaseModel):
    foo: str
    nested: list[MyNestedDatabaseModel]


def test_model_hierarchy_is_correct():
    my_doubly_nested_database_model = MyDoublyNestedDatabaseModel(baz="qux")
    my_nested_database_model = MyNestedDatabaseModel(bar="baz", doubly_nested=my_doubly_nested_database_model)
    my_database_model = MyDatabaseModel(foo="bar", nested=my_nested_database_model)
    assert my_doubly_nested_database_model._parent == my_nested_database_model
    assert my_nested_database_model._parent == my_database_model

    assert not my_doubly_nested_database_model.was_modified
    assert not my_nested_database_model.was_modified
    assert not my_database_model.was_modified
    my_doubly_nested_database_model.baz = "quux"
    assert my_doubly_nested_database_model.was_modified
    assert my_nested_database_model.was_modified
    assert my_database_model.was_modified


def test_model_hierarchy_is_correct_with_list():
    my_doubly_nested_database_model = MyDoublyNestedDatabaseModel(baz="qux")
    my_other_doubly_nested_database_model = MyDoublyNestedDatabaseModel(baz="qux")
    my_nested_database_model = MyNestedDatabaseModel(bar="baz", doubly_nested=my_doubly_nested_database_model)
    my_other_nested_database_model = MyNestedDatabaseModel(
        bar="baz", doubly_nested=my_other_doubly_nested_database_model
    )
    my_database_model = MyDatabaseModelWithList(
        foo="bar", nested=[my_nested_database_model, my_other_nested_database_model]
    )
    assert my_doubly_nested_database_model._parent == my_nested_database_model
    assert my_nested_database_model._parent == my_database_model
    assert my_other_nested_database_model._parent == my_database_model
