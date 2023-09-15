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


class InvalidIndexNameError(Exception):
    pass


class IncorrectSortKeyError(Exception):
    pass


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

    def get_item(self, id: str, model_class: Type[DatabaseModel], sort_key: Optional[Any] = None):
        """
        Returns an item from the database by id, using the partition key of the table.
        :param id: The id of the item to retrieve.
        :param model_class: The model class to use to deserialize the item.
        :param sort_key: The sort key of the item to retrieve. If the table does not have a sort key, this parameter should not be provided.
        """
        key = {self.table.key_schema.hash_key: id}
        if sort_key:
            key[self.table.key_schema.sort_key] = self._serialize_value(sort_key)
        return model_class(**self._get_dynamodb_table().get_item(Key=key)["Item"])

    def put_item(self, model: DatabaseModel):
        """
        Puts an item into the database.

        Before putting the item to the database, this method automatically constructs the index fields for the item,
        based on the configured indexes on the table. If the item already has a value for the index field, the field's
        value is going to be used instead.
        """
        data = self._get_item_data(model)
        self._get_dynamodb_table().put_item(Item=data)

    def batch_write(self):
        """
        Returns a context manager for batch writing items to the database. This method handles all the buffering of the
        batch operation and the construction of index fields for each item.
        """
        return BatchWriteContext(self)

    def query_index(
        self,
        hash_key: Condition,
        range_key: Condition,
        model_class: Type[DatabaseModel],
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ):
        """
        Queries the database using the provided hash key and range key conditions. A filter condition can also be provided
        using the filter_condition parameter. The method returns a list of items matching the query, deserialized into the
        provided model_class parameter.

        This method automatically constructs values for the index fields of the provided model_class, based on the configured
        indexes on the table. If the model_class already has a value for the index field, the field's value is going to be used,
        otherwise the value is going to be constructed based on the model's fields. If a type field is not provided on the model class
        or the model instance, the type field is going to be set to the model class name. In case of string values, the values are
        always prefixed with the type of the model to avoid collisions between different model types.

        Example:
            class Card(DatabaseModel):
                id: str
                player_id: IndexPrimaryKeyField
                type: IndexSecondaryKeyField
                tier: IndexSecondaryKeyField

            table = Table(indexes=[GSI(hash_key=Key("gsi_pk"), sort_key=Key("gsi_sk"))])

            The constructed index fields for the Card model are going to be:
                gsi_pk: <player_id>
                gsi_sk: card|<tier>

            A setup like this allows for queries to select for all cards of a player with a specific tier, avoiding potential
            collisions with models where the hash_key is also the player_id.

        :param hash_key: The hash key condition to use for the query. See statikk.conditions.Condition for more information.
        :param range_key: The range key condition to use for the query. See statikk.conditions.Condition for more information.
        :param model_class: The model class to use to deserialize the items.
        :param filter_condition: An optional filter condition to use for the query. See boto3.dynamodb.conditions.ComparisonCondition for more information.
        :param index_name: The name of the index to use for the query. If not provided, the first index configured on the table is used.
        """
        results = []
        if not index_name:
            index_name = self.table.indexes[0].name
        index_filter = [idx for idx in self.table.indexes if idx.name == index_name]
        if not index_filter:
            raise InvalidIndexNameError(f"The provided index name '{index_name}' is not configured on the table.")
        index = index_filter[0]
        key_condition = hash_key.evaluate(index.hash_key.name)
        if range_key is not None:
            key_condition = key_condition & range_key.evaluate(index.sort_key.name)

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
                if hasattr(item, key) and getattr(item, key) is not None:
                    continue
                if value is not None:
                    setattr(item, key, value)
            item.model_rebuild(force=True)
        data = item.model_dump()
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        return data

    def _serialize_value(self, value: Any):
        if isinstance(value, datetime):
            return int(value.timestamp())
        return value

    def _set_index_fields(self, model: DatabaseModel | Type[DatabaseModel], idx: GSI):
        model_fields = model.model_fields
        if idx.hash_key.name not in model_fields:
            model_fields[idx.hash_key.name] = FieldInfo(annotation=idx.hash_key.type, default=None, required=False)
        if idx.sort_key.name not in model_fields:
            model_fields[idx.sort_key.name] = FieldInfo(annotation=idx.sort_key.type, default=None, required=False)

    def _compose_index_values(self, model: DatabaseModel, idx: GSI) -> Dict[str, Any]:
        model_fields = model.model_fields
        hash_key_field = [
            field_name
            for field_name, field_info in model_fields.items()
            if field_info.annotation is not None
            if field_info.annotation is IndexPrimaryKeyField and idx.name in getattr(model, field_name).index_names
        ][0]
        sort_key_fields = [
            field_name
            for field_name, field_info in model_fields.items()
            if field_info.annotation is not None
            if field_info.annotation is IndexSecondaryKeyField and idx.name in getattr(model, field_name).index_names
        ]

        def _get_sort_key_value():
            if len(sort_key_fields) == 0:
                return None
            if len(sort_key_fields) == 1:
                value = getattr(model, sort_key_fields[0]).value
                if type(value) is not idx.sort_key.type:
                    raise IncorrectSortKeyError(
                        f"Incorrect sort key type. Sort key type for sort key '{idx.sort_key.name}' should be: "
                        + str(idx.sort_key.type)
                        + " but got: "
                        + str(type(value))
                    )
                value = value or idx.sort_key.default
                return self._serialize_value(value)

            if "type" not in sort_key_fields:
                sort_key_fields.insert(0, "type")
            sort_key_values: List[str] = []
            for field in sort_key_fields:
                value = getattr(model, field).value
                sort_key_values.append(value)
            return self.table.delimiter.join(sort_key_values)

        return {
            idx.hash_key.name: getattr(model, hash_key_field).value,
            idx.sort_key.name: _get_sort_key_value(),
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
