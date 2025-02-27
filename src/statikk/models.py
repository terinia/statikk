from __future__ import annotations

import typing
from uuid import uuid4
from typing import Optional, List, Any, Set, Type

from boto3.dynamodb.conditions import ComparisonCondition
from pydantic import BaseModel, model_serializer, model_validator
from pydantic.fields import FieldInfo, Field
from pydantic_core._pydantic_core import PydanticUndefined

from statikk.conditions import Condition
from statikk.expressions import DatabaseModelUpdateExpressionBuilder

if typing.TYPE_CHECKING:
    from statikk.engine import Table


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


class IndexFieldConfig(BaseModel):
    pk_fields: list[str] = []
    sk_fields: list[str] = []


class DatabaseModel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))

    @classmethod
    def type(cls) -> str:
        return cls.__name__

    @classmethod
    def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {"main_index": IndexFieldConfig(pk_fields=[], sk_fields=[])}

    @classmethod
    def set_table_ref(cls, table: "Table"):
        cls._table = table

    @classmethod
    def batch_write(cls):
        return cls._table.batch_write()

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

    def update(self, sort_key: Optional[str] = None) -> DatabaseModelUpdateExpressionBuilder:
        return DatabaseModelUpdateExpressionBuilder(self, sort_key)

    @classmethod
    def update_item(
        cls,
        hash_key: str,
        update_builder: DatabaseModelUpdateExpressionBuilder,
        model: DatabaseModel,
        range_key: Optional[str] = None,
    ):
        return cls._table.update_item(
            hash_key,
            range_key=range_key,
            update_builder=update_builder,
            model=model,
        )

    @classmethod
    def get(cls, id: str, sort_key: Optional[str] = None, consistent_read: bool = False):
        return cls._table.get_item(id=id, model_class=cls, sort_key=sort_key, consistent_read=consistent_read)

    @classmethod
    def batch_get(cls, ids: List[str], batch_size: int = 100):
        return cls._table.batch_get_items(ids=ids, model_class=cls, batch_size=batch_size)

    @classmethod
    def scan(
        cls,
        filter_condition: Optional[ComparisonCondition] = None,
        consistent_read: bool = False,
    ):
        return cls._table.scan(model_class=cls, filter_condition=filter_condition)

    @model_serializer(mode="wrap")
    def serialize_model(self, handler):
        data = handler(self)
        data["type"] = self.type()
        return data

    def get_attribute(self, attribute_name: str):
        if attribute_name == "type":
            return self.type()
        return getattr(self, attribute_name)
