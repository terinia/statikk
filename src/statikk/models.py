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


class IndexPrimaryKeyField(Index):
    pass


class IndexSecondaryKeyField(Index):
    order: Optional[int] = None


class DatabaseModel(BaseModel):
    id: str

    @classmethod
    def model_type(cls):
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
            if field.annotation is IndexSecondaryKeyField:
                extra_fields["order"] = field.default.order
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

    @model_validator(mode="after")
    def validate_index_fields(self):
        sort_key_field_orders = [
            getattr(self, field_name).order
            for field_name, field_info in self.model_fields.items()
            if field_info.annotation is not None
            if field_info.annotation is IndexSecondaryKeyField
        ]
        have_orders_defined = any([order for order in sort_key_field_orders])
        all_orders_defined = all([order for order in sort_key_field_orders])
        use_default_ordering = all([order is None for order in sort_key_field_orders])
        if have_orders_defined and not all_orders_defined and not use_default_ordering:
            raise ValueError(
                f"`order` is not defined on at least one of the Index keys on model {type(self)}. Please set the order for all sort key fields."
            )
        if all_orders_defined and len(set(sort_key_field_orders)) != len(sort_key_field_orders):
            raise ValueError(
                f"Duplicate `order` values found on model {type(self)}. Please ensure that all `order` values are unique."
            )
