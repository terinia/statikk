from __future__ import annotations

import uuid
from typing import Optional, List, TypeVar, Generic, Any, Set, Type

from pydantic import BaseModel, Field, model_serializer, model_validator
from pydantic_core._pydantic_core import PydanticUndefined
from pydantic.fields import FieldInfo


class GSI(BaseModel):
    name: str = "main-index"
    hash_key: str
    sort_key: Optional[str] = None


class KeySchema(BaseModel):
    hash_key: str
    sort_key: Optional[str] = None


class Table(BaseModel):
    name: str
    key_schema: KeySchema
    indexes: List[GSI] = Field(default_factory=list)
    delimiter: str = "|"


T = TypeVar("T")


class Index(BaseModel, Generic[T]):
    value: Optional[T] = None
    index_names: Optional[List[str]] = ["main-index"]

    @model_serializer
    def ser_model(self) -> T:
        return self.value


class IndexPrimaryKeyField(Index[T], Generic[T]):
    pass


class IndexSecondaryKeyField(Index[T], Generic[T]):
    pass


class DatabaseModel(BaseModel):
    id: str
    type: str = None

    @staticmethod
    def _index_types() -> Set[Type[Index]]:
        return {Index, IndexPrimaryKeyField, IndexSecondaryKeyField}

    @staticmethod
    def _is_index_field(field: FieldInfo) -> bool:
        annotation_bases = set(field.annotation.__bases__) if field.annotation else {}
        index_types = {Index, IndexPrimaryKeyField, IndexSecondaryKeyField}
        return len(index_types.intersection(annotation_bases)) > 0

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
            if "type" not in data:
                data["type"] = cls.__name__

            for key, value in data.items():
                field = cls.model_fields[key]
                if cls._is_index_field(field) and not isinstance(value, dict):
                    if isinstance(value, tuple(cls._index_types())):
                        continue
                    else:
                        data[key] = cls._create_index_field_from_shorthand(field, value)
        return data
