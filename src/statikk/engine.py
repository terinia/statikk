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

    def _dynamodb_client(self):
        return boto3.client(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )

    def _get_dynamodb_table(self):
        dynamodb = boto3.resource(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )
        return dynamodb.Table(self.table.name)

    def get_item(self, id: str, model_class: Type[DatabaseModel]):
        return model_class(**self._get_dynamodb_table().get_item(Key={"id": id})["Item"])

    def put_item(self, model: DatabaseModel):
        data = self._get_item_data(model)
        self._get_dynamodb_table().put_item(Item=data)

    def batch_write(self):
        return BatchWriteContext(self)

    def query_index(
        self,
        index_name: str,
        hash_key: Condition,
        range_key: Condition,
        model_class: Type[DatabaseModel],
        filter_condition: Optional[ComparisonCondition] = None,
    ):
        results = []
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
        last_evaluated_key = True

        while last_evaluated_key:
            items = self._get_dynamodb_table().query(**query_params)
            results.extend([model_class(**item) for item in items["Items"]])
            last_evaluated_key = items.get("LastEvaluatedKey", False)

        return results

    def _convert_dynamodb_to_python(self, item) -> Dict[str, Any]:
        result = {}
        for key, value in item.items():
            if isinstance(value, dict) and len(value) == 1:
                dynamodb_type = list(value.keys())[0]
                dynamodb_value = value[dynamodb_type]

                if dynamodb_type == "S":
                    result[key] = dynamodb_value
                elif dynamodb_type == "N":
                    result[key] = int(dynamodb_value)
                elif dynamodb_type == "BOOL":
                    result[key] = dynamodb_value
                elif dynamodb_type == "L":
                    result[key] = [self._convert_dynamodb_to_python(item) for item in dynamodb_value]
                elif dynamodb_type == "M":
                    result[key] = self._convert_dynamodb_to_python(dynamodb_value)
            else:
                result[key] = value
        return result

    def batch_get_items(
        self, ids: List[str], model_class: Type[DatabaseModel], batch_size: int = 100
    ) -> List[DatabaseModel]:
        dynamodb = self._dynamodb_client()

        id_batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]

        results = []

        for batch in id_batches:
            request_items = {
                self.table.name: {
                    "Keys": [{"id": {"S": id}} for id in batch],
                }
            }

            while request_items:
                response = dynamodb.batch_get_item(RequestItems=request_items)

                if "UnprocessedKeys" in response and response["UnprocessedKeys"]:
                    request_items = response["UnprocessedKeys"][self.table.name]
                else:
                    results.extend(
                        [
                            model_class(**self._convert_dynamodb_to_python(item))
                            for item in response["Responses"][self.table.name]
                        ]
                    )
                    break

        return results

    def _get_item_data(self, item: DatabaseModel):
        for idx in self.table.indexes:
            index_fields = self._compose_index_values(item, idx)
            for key, value in index_fields.items():
                if value is not None:
                    setattr(item, key, value)
            item.model_rebuild(force=True)
        data = item.model_dump()
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        return data

    def _serialize_value(self, value: Any):
        if isinstance(value, datetime):
            return str(value)
        return value

    def _set_index_fields(self, model: DatabaseModel | Type[DatabaseModel], idx: GSI):
        model_fields = model.model_fields
        if idx.hash_key not in model_fields:
            model_fields[idx.hash_key] = FieldInfo(annotation=str, default=None, required=False)
        if idx.sort_key not in model_fields:
            model_fields[idx.sort_key] = FieldInfo(annotation=str, default=None, required=False)

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
        if "type" not in sort_key_fields:
            sort_key_fields.insert(0, "type")

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

    def _perform_batch_write(self, put_items: List[DatabaseModel], delete_items: List[DatabaseModel]):
        if len(put_items) == 0 and len(delete_items) == 0:
            return

        dynamodb_table = self._get_dynamodb_table()

        if len(put_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in put_items:
                    data = self._get_item_data(item)
                    batch.put_item(Item=data)

        if len(delete_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in delete_items:
                    data = self._get_item_data(item)
                    batch.delete_item(Key=data)


class BatchWriteContext:
    def __init__(self, app: SingleTableApplication):
        self._app = app
        self._put_items: List[DatabaseModel] = []
        self._delete_items: List[DatabaseModel] = []

    def put(self, item: DatabaseModel):
        self._put_items.append(item)

    def delete(self, item: DatabaseModel):
        self._delete_items.append(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._app._perform_batch_write(self._put_items, self._delete_items)
