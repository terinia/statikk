import os
from datetime import datetime
from typing import Any, Dict, Type
from pydantic import Field
import boto3
from botocore.config import Config

from src.statikk.models import (
    Table,
    DatabaseModel,
    GSI,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
)


class SingleTableApplication:
    def __init__(self, table: Table):
        self.table = table

    def _get_dynamodb_client(self):
        dynamodb = boto3.resource(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )
        return dynamodb.Table(self.table.name)

    def get_item(self, id: str, model_class: Type[DatabaseModel]):
        return model_class(**self._get_dynamodb_client().get_item(Key={"id": id})["Item"])

    def put_item(self, model: DatabaseModel):
        for idx in self.table.indexes:
            self._set_index_fields(model, idx)
            index_fields = self._compose_index_values(model, idx)
            for key, value in index_fields.items():
                if value is not None:
                    setattr(model, key, value)
            model.model_rebuild(force=True)
        data = model.model_dump()
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        self._get_dynamodb_client().put_item(Item=data)

    def _set_index_fields(self, model: DatabaseModel, idx: GSI):
        model_fields = model.model_fields
        if idx.hash_key not in model_fields:
            model_fields[idx.hash_key] = Field(default=None)
        if idx.sort_key not in model_fields:
            model_fields[idx.sort_key] = Field(default=None)

    def _serialize_value(self, value: Any):
        if isinstance(value, datetime):
            return str(value)
        return value

    def _compose_index_values(self, model: DatabaseModel, idx: GSI) -> Dict[str, Any]:
        model_fields = model.model_fields
        hash_key_field = [
            field_name
            for field_name, field_info in model_fields.items()
            if field_info.annotation is not None
            if set(field_info.annotation.__bases__).intersection({IndexPrimaryKeyField})
            and idx.name in getattr(model, field_name).index_names
        ][0]
        sort_key_fields = [
            field_name
            for field_name, field_info in model_fields.items()
            if field_info.annotation is not None
            if set(field_info.annotation.__bases__).intersection({IndexSecondaryKeyField})
            and idx.name in getattr(model, field_name).index_names
        ]

        def _get_sort_key_value():
            if len(sort_key_fields) == 0:
                return None
            if len(sort_key_fields) == 1:
                return getattr(model, sort_key_fields[0]).value
            return self.table.delimiter.join([str(getattr(model, field).value) for field in sort_key_fields])

        return {
            idx.hash_key: getattr(model, hash_key_field).value,
            idx.sort_key: _get_sort_key_value(),
        }
