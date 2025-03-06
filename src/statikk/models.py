from __future__ import annotations

import typing
import logging
from uuid import uuid4
from typing import Optional, List, Any, Set, Type
from statikk.typing import T

from boto3.dynamodb.conditions import ComparisonCondition
from pydantic import BaseModel, model_serializer, model_validator, Field, Extra
from pydantic.fields import FieldInfo, Field
from pydantic_core._pydantic_core import PydanticUndefined

from statikk.conditions import Condition
from statikk.expressions import DatabaseModelUpdateExpressionBuilder
from statikk.fields import FIELD_STATIKK_TYPE, FIELD_STATIKK_PARENT_ID

if typing.TYPE_CHECKING:
    from statikk.engine import Table

logger = logging.getLogger(__name__)


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


class TrackingMixin:
    _original_hash: int = Field(exclude=True)

    @property
    def should_track(self) -> bool:
        if self._parent is not None:
            return self._parent.should_track
        return False

    def init_tracking(self):
        self._original_hash = self._recursive_hash()

    def _recursive_hash(self) -> int:
        """
        Compute a hash value for the model, ignoring nested DatabaseModel instances.

        This ensures that changes to child models don't affect the parent's hash.

        Returns:
            A hash value representing the model's non-model fields.
        """
        if not self.should_track:
            return 0

        values = []
        for field_name in self.model_fields:
            if not hasattr(self, field_name):
                continue

            if field_name.startswith("_"):
                continue

            value = getattr(self, field_name)

            if hasattr(value, "__class__") and issubclass(value.__class__, DatabaseModel):
                continue

            if isinstance(value, list) or isinstance(value, set):
                contains_model = False
                for item in value:
                    if hasattr(item, "__class__") and issubclass(item.__class__, DatabaseModel):
                        contains_model = True
                        break
                if contains_model:
                    continue

            if isinstance(value, dict):
                contains_model = False
                if not contains_model:
                    for val in value.values():
                        if hasattr(val, "__class__") and issubclass(val.__class__, DatabaseModel):
                            contains_model = True
                            break
                if contains_model:
                    continue

            hashed_value = self._make_hashable(value)
            if hashed_value is not None:
                values.append(hashed_value)

        return hash(tuple(values))

    def _make_hashable(self, value: Any) -> Any:
        if isinstance(value, (str, int, float, bool, type(None))):
            return value
        elif isinstance(value, list) or isinstance(value, set):
            return tuple(self._make_hashable(item) for item in value)
        elif isinstance(value, dict):
            return tuple((self._make_hashable(k), self._make_hashable(v)) for k, v in sorted(value.items()))
        elif isinstance(value, BaseModel) and hasattr(value, "_recursive_hash"):
            return value._recursive_hash()
        else:
            try:
                return hash(value)
            except TypeError:
                logger.warning(
                    f"{type(value)} is unhashable, tracking will not work. Consider implementing the TrackingMixin for this type."
                )
                return None
        return value

    @property
    def was_modified(self) -> bool:
        if self.should_track:
            return self._recursive_hash() != self._original_hash

        return True


class DatabaseModel(BaseModel, TrackingMixin, extra=Extra.allow):
    id: str = Field(default_factory=lambda: str(uuid4()))
    _parent: Optional[DatabaseModel] = None
    _model_types_in_hierarchy: dict[str, Type[DatabaseModel]] = {}

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
    def is_nested(cls) -> bool:
        return False

    @property
    def is_simple_object(self) -> bool:
        return len(self._model_types_in_hierarchy) == 1

    @classmethod
    def query(
        cls: Type[T],
        hash_key: Condition,
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ) -> list[T]:
        return cls._table.query_index(
            hash_key=hash_key,
            model_class=cls,
            range_key=range_key,
            filter_condition=filter_condition,
            index_name=index_name,
        )

    @classmethod
    def query_hierarchy(
        cls: Type[T],
        hash_key: Union[Condition | str],
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ) -> T:
        return cls._table.query_hierarchy(
            hash_key=hash_key,
            model_class=cls,
            range_key=range_key,
            filter_condition=filter_condition,
            index_name=index_name,
        )

    def save(self):
        return self._table.put_item(self)

    def delete(self):
        return self._table.delete_item(self)

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
    def get(cls: Type[T], id: str, sort_key: Optional[str] = None, consistent_read: bool = False) -> T:
        return cls._table.get_item(id=id, model_class=cls, sort_key=sort_key, consistent_read=consistent_read)

    @classmethod
    def batch_get(cls: Type[T], ids: List[str], batch_size: int = 100) -> list[T]:
        return cls._table.batch_get_items(ids=ids, model_class=cls, batch_size=batch_size)

    def should_write_to_database(self) -> bool:
        if self.is_nested():
            return self._parent.should_write_to_database()
        return True

    @classmethod
    def scan(
        cls,
        filter_condition: Optional[ComparisonCondition] = None,
        consistent_read: bool = False,
    ) -> list[DatabaseModel]:
        return cls._table.scan(filter_condition=filter_condition)

    @model_serializer(mode="wrap")
    def serialize_model(self, handler):
        data = handler(self)
        data[FIELD_STATIKK_TYPE] = self.type()
        if self._parent:
            data[FIELD_STATIKK_PARENT_ID] = self._parent.id
        return data

    @model_validator(mode="after")
    def initialize_tracking(self):
        self._model_types_in_hierarchy[self.type()] = type(self)
        if not self.is_nested():
            self._set_parent_references(self)
        self.init_tracking()

        return self

    def split_to_simple_objects(self, items: Optional[list[DatabaseModel]] = None) -> list[DatabaseModel]:
        """
        Split a complex nested DatabaseModel into a list of individual DatabaseModel instances.

        This method recursively traverses the model and all its nested DatabaseModel instances,
        collecting them into a flat list for simpler processing or storage.

        Args:
            items: An optional existing list to add items to. If None, a new list is created.

        Returns:
            A list containing this model and all nested DatabaseModel instances.
        """
        if items is None:
            items = [self]
        else:
            if self not in items:
                items.append(self)

        # Iterate through all fields of the model
        for field_name, field_value in self:
            # Skip fields that start with underscore (private fields)
            if field_name.startswith("_"):
                continue

            # Handle direct DatabaseModel instances
            if hasattr(field_value, "__class__") and issubclass(field_value.__class__, DatabaseModel):
                if field_value not in items:
                    items.append(field_value)
                field_value.split_to_simple_objects(items)

            # Handle lists containing DatabaseModel instances
            elif isinstance(field_value, list):
                for item in field_value:
                    if hasattr(item, "__class__") and issubclass(item.__class__, DatabaseModel):
                        if item not in items:
                            items.append(item)
                        item.split_to_simple_objects(items)

            # Handle sets containing DatabaseModel instances
            elif isinstance(field_value, set):
                for item in field_value:
                    if hasattr(item, "__class__") and issubclass(item.__class__, DatabaseModel):
                        if item not in items:
                            items.append(item)
                        item.split_to_simple_objects(items)

            # Handle dictionaries that may contain DatabaseModel instances
            elif isinstance(field_value, dict):
                # Check dictionary values
                for value in field_value.values():
                    if hasattr(value, "__class__") and issubclass(value.__class__, DatabaseModel):
                        if value not in items:
                            items.append(value)
                        value.split_to_simple_objects(items)

        return items

    def get_attribute(self, attribute_name: str):
        if attribute_name == FIELD_STATIKK_TYPE:
            return self.type()
        return getattr(self, attribute_name)

    def get_nested_model_fields(self) -> set[DatabaseModel]:
        nested_models = []
        for field_name, field_value in self:
            if issubclass(field_value.__class__, DatabaseModel) and field_value.is_nested():
                nested_models.append(field_name)
            elif isinstance(field_value, list):
                for item in field_value:
                    if issubclass(item.__class__, DatabaseModel) and item.is_nested():
                        nested_models.append(field_name)
            elif isinstance(field_value, set):
                for item in field_value:
                    if issubclass(item.__class__, DatabaseModel) and item.is_nested():
                        nested_models.append(field_name)
            elif isinstance(field_value, dict):
                for key, value in field_value.items():
                    if issubclass(value.__class__, DatabaseModel) and value.is_nested():
                        nested_models.append(field_name)
        return set(nested_models)

    def get_type_from_hierarchy_by_name(self, name: str) -> Optional[Type[DatabaseModel]]:
        return self._model_types_in_hierarchy.get(name)

    def _set_parent_to_field(self, field: DatabaseModel, parent: DatabaseModel, root: DatabaseModel):
        if field._parent:
            return  # Already set
        field._parent = parent
        root._model_types_in_hierarchy[field.type()] = type(field)
        field._set_parent_references(root)
        field.init_tracking()

    def _set_parent_references(self, root: DatabaseModel):
        for field_name, field_value in self:
            if isinstance(field_value, DatabaseModel):
                self._set_parent_to_field(field_value, self, root)
            elif isinstance(field_value, list):
                for item in field_value:
                    if isinstance(item, DatabaseModel):
                        self._set_parent_to_field(item, self, root)
            elif isinstance(field_value, set):
                for item in field_value:
                    if isinstance(item, DatabaseModel):
                        self._set_parent_to_field(item, self, root)
            elif isinstance(field_value, dict):
                for key, value in field_value.items():
                    if isinstance(value, DatabaseModel):
                        self._set_parent_to_field(value, self, root)
