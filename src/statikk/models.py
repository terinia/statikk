from __future__ import annotations

import uuid
from typing import Optional, List, TypeVar, Generic, Any

from pydantic import BaseModel, Field, model_serializer, model_validator
from pydantic_core._pydantic_core import PydanticUndefined


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
                annotation_bases = (
                    set(cls.model_fields[key].annotation.__bases__) if cls.model_fields[key].annotation else {}
                )
                index_types = {Index, IndexPrimaryKeyField, IndexSecondaryKeyField}
                if index_types.intersection(annotation_bases) and not isinstance(value, dict):
                    if isinstance(value, tuple(index_types)):
                        continue
                    else:
                        field = cls.model_fields[key]
                        annotation = field.annotation
                        extra_fields = dict()
                        if field.default is not PydanticUndefined:
                            extra_fields["index_names"] = field.default.index_names
                        data[key] = annotation(value=value, **extra_fields)
        return data
