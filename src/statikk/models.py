from __future__ import annotations

import uuid
from typing import Optional, List, Any, Set, Type

from boto3.dynamodb.conditions import ComparisonCondition
from pydantic import BaseModel, model_serializer, model_validator
from pydantic.fields import FieldInfo
from pydantic_core._pydantic_core import PydanticUndefined

from statikk.conditions import Condition
from statikk.expressions import DatabaseModelUpdateExpressionBuilder


class Key(BaseModel):
    name: str
    type: Type = str
    default: Optional[Any] = ""


class GSI(BaseModel):
    name: str = "main-index"
    hash_key: Key
    sort_key: Key
    projection_type: str = "ALL"


class KeySchema(BaseModel):
    hash_key: str
    sort_key: Optional[str] = None


class Index(BaseModel):
    value: Optional[Any] = None
    index_names: Optional[List[str]] = ["main-index"]

    @model_serializer
    def ser_model(self) -> Any:
        return self.value

    def __getattr__(self, attr: str) -> Any:
        return super().__getattribute__(attr)


class IndexPrimaryKeyField(Index):
    pass


class IndexSecondaryKeyField(Index):
    pass


class DatabaseModel(BaseModel):
    id: str

    @classmethod
    def type(cls):
        return cls.__name__

    @classmethod
    def include_type_in_sort_key(cls):
        return True

    @classmethod
    def type_is_primary_key(cls):
        return False

    @classmethod
    def set_table_ref(cls, table):
        cls._table = table

    @classmethod
    def batch_write(cls):
        return cls._table.batch_write()

    class Config:
        include_type_field_in_sort_key = False

    @classmethod
    def query(
        cls,
        hash_key: Condition,
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ):
        return cls._table.query_index(
            hash_key=hash_key,
            model_class=cls,
            range_key=range_key,
            filter_condition=filter_condition,
            index_name=index_name,
        )

    def save(self):
        return self._table.put_item(self)

    def delete(self):
        return self._table.delete_item(self.id)

    @classmethod
    def update(cls, id: str, sort_key: Optional[str] = None) -> DatabaseModelUpdateExpressionBuilder:
        return DatabaseModelUpdateExpressionBuilder(cls, id, sort_key)

    @classmethod
    def update_item(
        cls,
        hash_key: str,
        update_builder: DatabaseModelUpdateExpressionBuilder,
        range_key: Optional[str] = None,
    ):
        return cls._table.update_item(hash_key, range_key=range_key, update_builder=update_builder)

    @classmethod
    def get(cls, id: str, sort_key: Optional[str] = None):
        return cls._table.get_item(id=id, model_class=cls, sort_key=sort_key)

    @classmethod
    def batch_get(cls, ids: List[str], batch_size: int = 100):
        return cls._table.batch_get_items(ids=ids, model_class=cls, batch_size=batch_size)

    @classmethod
    def scan(cls, filter_condition: Optional[ComparisonCondition] = None):
        return cls._table.scan(model_class=cls, filter_condition=filter_condition)

    @staticmethod
    def _index_types() -> Set[Type[Index]]:
        return {Index, IndexPrimaryKeyField, IndexSecondaryKeyField}

    @staticmethod
    def _is_index_field(field: FieldInfo) -> bool:
        index_types = {Index, IndexPrimaryKeyField, IndexSecondaryKeyField}
        return field.annotation in index_types

    @classmethod
    def _create_index_field_from_shorthand(cls, field: FieldInfo, value: str) -> FieldInfo:
        annotation = field.annotation
        extra_fields = dict()
        if field.default is not PydanticUndefined:
            extra_fields["index_names"] = field.default.index_names
        return annotation(value=value, **extra_fields)

    @model_validator(mode="before")
    @classmethod
    def check_boxed_indexes(cls, data: Any) -> Any:
        """
        This method allows for a cleaner interface when working with model indexes.
        Instead of having to manually box the index value, this method will do it for you.
        For example:
          my_model = MyModel(id="123", my_index="abc")
        Is equivalent to:
          my_model = MyModel(id="123", my_index=IndexPrimaryKeyField(value="abc"))
        """
        if isinstance(data, dict):
            if "id" not in data:
                data["id"] = str(uuid.uuid4())

            for key, value in data.items():
                field = cls.model_fields[key]
                if cls._is_index_field(field) and not isinstance(value, dict):
                    if isinstance(value, tuple(cls._index_types())):
                        continue
                    else:
                        data[key] = cls._create_index_field_from_shorthand(field, value)
        return data
