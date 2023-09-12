import os
from datetime import datetime
from typing import Any, Dict, Type, Optional, List

import boto3
from botocore.config import Config
from pydantic.fields import FieldInfo
from boto3.dynamodb.conditions import ComparisonCondition

from statikk.conditions import Condition
from statikk.models import (
    Table,
    DatabaseModel,
    GSI,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
)


class SingleTableApplication:
    def __init__(self, table: Table, models: List[Type[DatabaseModel]]):
        self.table = table
        self.models = models
        for idx in self.table.indexes:
            for model in self.models:
                self._set_index_fields(model, idx)

    def _get_dynamodb_table(self):
        dynamodb = boto3.resource(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )
        return dynamodb.Table(self.table.name)

    def get_item(self, id: str, model_class: Type[DatabaseModel]):
        return model_class(**self._get_dynamodb_table().get_item(Key={"id": id})["Item"])

    def put_item(self, model: DatabaseModel):
        for idx in self.table.indexes:
            index_fields = self._compose_index_values(model, idx)
            for key, value in index_fields.items():
                if value is not None:
                    setattr(model, key, value)
            model.model_rebuild(force=True)
        data = model.model_dump()
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        self._get_dynamodb_table().put_item(Item=data)

    def query_index(
        self,
        index_name: str,
        hash_key: Condition,
        range_key: Condition,
        model_class: Type[DatabaseModel],
        filter_condition: Optional[ComparisonCondition] = None,
    ):
        index = [idx for idx in self.table.indexes if idx.name == index_name][0]
        key_condition = hash_key.evaluate(index.hash_key)
        if range_key is not None:
            key_condition = key_condition & range_key.evaluate(index.sort_key)

        query_params = {
            "IndexName": index_name,
            "KeyConditionExpression": key_condition,
        }
        if filter_condition:
            query_params["FilterExpression"] = filter_condition
        items = self._get_dynamodb_table().query(**query_params)

        return [model_class(**item) for item in items["Items"]]

    def _set_index_fields(self, model: DatabaseModel | Type[DatabaseModel], idx: GSI):
        model_fields = model.model_fields
        if idx.hash_key not in model_fields:
            model_fields[idx.hash_key] = FieldInfo(annotation=str, default=None, required=False)
        if idx.sort_key not in model_fields:
            model_fields[idx.sort_key] = FieldInfo(annotation=str, default=None, required=False)

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
