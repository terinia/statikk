import os
from datetime import datetime
from typing import Any, Dict, Type, Optional, List, Union

import boto3
from botocore.config import Config
from pydantic.fields import FieldInfo
from boto3.dynamodb.conditions import ComparisonCondition, Key
from boto3.dynamodb.types import TypeDeserializer

from statikk.conditions import Condition, Equals, BeginsWith
from statikk.expressions import UpdateExpressionBuilder
from statikk.models import (
    DatabaseModel,
    GSI,
    IndexPrimaryKeyField,
    IndexSecondaryKeyField,
    KeySchema,
)


class InvalidIndexNameError(Exception):
    pass


class IncorrectSortKeyError(Exception):
    pass


class ItemNotFoundError(Exception):
    pass


class Table:
    def __init__(
        self,
        name: str,
        models: List[Type[DatabaseModel]],
        key_schema: KeySchema,
        indexes: List[GSI] = Optional[None],
        delimiter: str = "|",
        billing_mode: str = "PAY_PER_REQUEST",
    ):
        self.name = name
        self.key_schema = key_schema
        self.indexes = indexes or []
        self.delimiter = delimiter
        self.models = models
        self.billing_mode = billing_mode
        for idx in self.indexes:
            for model in self.models:
                self._set_index_fields(model, idx)
                model.set_table_ref(self)

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
        return dynamodb.Table(self.name)

    def _to_dynamodb_type(self, type: Any):
        if type is str:
            return "S"
        if type is int:
            return "N"
        if type is datetime:
            return "N"
        if type is float:
            return "N"
        if type is bool:
            return "BOOL"
        if type is list:
            return "L"
        if type is dict:
            return "M"
        if type is None:
            return "NULL"
        raise ValueError(f"Unsupported type: {type}.")

    def create(self, aws_region: Optional[str] = None):
        """Creates a DynamoDB table from the specified definition.

        This method only supports a subset of all available configuration values on DynamoDB tables.
        It is recommended that you manage your DynamoDb table outside the scope of your application, using CloudFormation or Terraform.

        This method is mostly here to provide a shorthand for boostrapping an in-memory DynamoDb table mock
        in moto-based unit and integration tests.
        """
        region = aws_region or os.environ.get("AWS_DEFAULT_REGION")
        if not region:
            raise ValueError(
                "AWS region not specified. Please provide a region or set the AWS_DEFAULT_REGION environment variable."
            )
        hash_key_attribute_definitions = [
            {
                "AttributeName": index.hash_key.name,
                "AttributeType": self._to_dynamodb_type(index.hash_key.type),
            }
            for index in self.indexes
        ]
        sort_key_attribute_definitions = [
            {
                "AttributeName": index.sort_key.name,
                "AttributeType": self._to_dynamodb_type(index.sort_key.type),
            }
            for index in self.indexes
        ]
        global_secondary_indexes = [
            {
                "IndexName": index.name,
                "KeySchema": [
                    {
                        "AttributeName": index.hash_key.name,
                        "KeyType": "HASH",
                    },
                    {
                        "AttributeName": index.sort_key.name,
                        "KeyType": "RANGE",
                    },
                ],
                "Projection": {"ProjectionType": index.projection_type},
            }
            for index in self.indexes
        ]
        key_schema = [{"AttributeName": self.key_schema.hash_key, "KeyType": "HASH"}]
        if self.key_schema.sort_key:
            key_schema.append({"AttributeName": self.key_schema.sort_key, "KeyType": "RANGE"})
        self._dynamodb_client().create_table(
            TableName=self.name,
            KeySchema=key_schema,
            AttributeDefinitions=[{"AttributeName": self.key_schema.hash_key, "AttributeType": "S"}]
            + hash_key_attribute_definitions
            + sort_key_attribute_definitions,
            GlobalSecondaryIndexes=global_secondary_indexes,
            BillingMode=self.billing_mode,
        )

    def delete(self):
        """Deletes the DynamoDB table."""
        self._dynamodb_client().delete_table(TableName=self.name)

    def delete_item(self, id: str):
        """
        Deletes an item from the database by id, using the partition key of the table.
        :param id: The id of the item to delete.
        """
        key = {self.key_schema.hash_key: id}
        self._get_dynamodb_table().delete_item(Key=key)

    def get_item(self, id: str, model_class: Type[DatabaseModel], sort_key: Optional[Any] = None):
        """
        Returns an item from the database by id, using the partition key of the table.
        :param id: The id of the item to retrieve.
        :param model_class: The model class to use to deserialize the item.
        :param sort_key: The sort key of the item to retrieve. If the table does not have a sort key, this parameter should not be provided.
        """
        key = {self.key_schema.hash_key: id}
        if sort_key:
            key[self.key_schema.sort_key] = self._serialize_value(sort_key)
        raw_data = self._get_dynamodb_table().get_item(Key=key)
        if "Item" not in raw_data:
            raise ItemNotFoundError(f"{model_class} with id '{id}' not found.")
        data = raw_data["Item"]
        del data["type"]
        return model_class(**data)

    def put_item(self, model: DatabaseModel) -> DatabaseModel:
        """
        Puts an item into the database.

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


        Returns the enriched database model instance.
        """
        data = self._serialize_item(model)
        self._get_dynamodb_table().put_item(Item=data)
        del data["type"]
        for key, value in data.items():
            data[key] = self._deserialize_value(value, model.model_fields[key])
        return type(model)(**data)

    def update_item(
        self,
        hash_key: str,
        update_builder: UpdateExpressionBuilder,
        range_key: Optional[str] = None,
    ):
        (
            update_expression,
            expression_attribute_values,
            expression_attribute_names,
        ) = update_builder.build()
        key = {self.key_schema.hash_key: hash_key}
        if range_key:
            key[self.key_schema.sort_key] = range_key

        request = {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
        }
        if expression_attribute_names:
            request["ExpressionAttributeNames"] = expression_attribute_names

        response = self._get_dynamodb_table().update_item(**request)
        return response

    def batch_write(self):
        """
        Returns a context manager for batch writing items to the database. This method handles all the buffering of the
        batch operation and the construction of index fields for each item.
        """
        return BatchWriteContext(self)

    def query_index(
        self,
        hash_key: Union[Condition | str],
        model_class: Type[DatabaseModel],
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ):
        """
        Queries the database using the provided hash key and range key conditions. A filter condition can also be provided
        using the filter_condition parameter. The method returns a list of items matching the query, deserialized into the
        provided model_class parameter.

        :param hash_key: The hash key condition to use for the query. See statikk.conditions.Condition for more information.
        :param range_key: The range key condition to use for the query. See statikk.conditions.Condition for more information.
        :param model_class: The model class to use to deserialize the items.
        :param filter_condition: An optional filter condition to use for the query. See boto3.dynamodb.conditions.ComparisonCondition for more information.
        :param index_name: The name of the index to use for the query. If not provided, the first index configured on the table is used.
        """
        if isinstance(hash_key, str):
            hash_key = Equals(hash_key)
        if not index_name:
            index_name = self.indexes[0].name
        index_filter = [idx for idx in self.indexes if idx.name == index_name]
        if not index_filter:
            raise InvalidIndexNameError(f"The provided index name '{index_name}' is not configured on the table.")
        index = index_filter[0]
        key_condition = hash_key.evaluate(index.hash_key.name)
        if range_key is None:
            range_key = BeginsWith(model_class.type())

        range_key.enrich(model_class=model_class)
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
            yield from [model_class(**self._remove_type_field_from_item(item)) for item in items["Items"]]
            last_evaluated_key = items.get("LastEvaluatedKey", False)

    def scan(
        self,
        model_class: Type[DatabaseModel],
        filter_condition: Optional[ComparisonCondition] = None,
    ):
        """
        Scans the database for items matching the provided filter condition. The method returns a list of items matching
        the query, deserialized into the provided model_class parameter.

        :param model_class: The model class to use to deserialize the items.
        :param filter_condition: An optional filter condition to use for the query. See boto3.dynamodb.conditions.ComparisonCondition for more information.
        """
        query_params = {}
        if filter_condition:
            query_params["FilterExpression"] = filter_condition
        last_evaluated_key = True

        while last_evaluated_key:
            items = self._get_dynamodb_table().scan(**query_params)
            yield from [model_class(**self._remove_type_field_from_item(item)) for item in items["Items"]]
            last_evaluated_key = items.get("LastEvaluatedKey", False)

    def _convert_dynamodb_to_python(self, item) -> Dict[str, Any]:
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in item.items()}

    def batch_get_items(
        self, ids: List[str], model_class: Type[DatabaseModel], batch_size: int = 100
    ) -> List[DatabaseModel]:
        dynamodb = self._dynamodb_client()

        id_batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]

        results = []

        for batch in id_batches:
            request_items = {
                self.name: {
                    "Keys": [{"id": {"S": id}} for id in batch],
                }
            }

            while request_items:
                response = dynamodb.batch_get_item(RequestItems=request_items)

                if "UnprocessedKeys" in response and response["UnprocessedKeys"]:
                    request_items = response["UnprocessedKeys"][self.name]
                else:
                    results.extend(
                        [
                            model_class(**self._convert_dynamodb_to_python(self._remove_type_field_from_item(item)))
                            for item in response["Responses"][self.name]
                        ]
                    )
                    break

        return results

    def _prepare_model_data(self, item: DatabaseModel) -> Dict[str, Any]:
        for idx in self.indexes:
            index_fields = self._compose_index_values(item, idx)
            for key, value in index_fields.items():
                if hasattr(item, key) and getattr(item, key) is not None:
                    continue
                if value is not None:
                    setattr(item, key, value)
            item.model_rebuild(force=True)
        return item.model_dump()

    def _serialize_item(self, item: DatabaseModel):
        data = self._prepare_model_data(item)
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        data["type"] = item.type()
        return data

    def _deserialize_item(self, item: DatabaseModel):
        data = self._prepare_model_data(item)
        for key, value in data.items():
            data[key] = self._deserialize_value(value, item.model_fields[key].annotation)
        data["type"] = item.type()
        return data

    def _deserialize_value(self, value: Any, annotation: Any):
        if annotation is datetime:
            return datetime.fromtimestamp(value)
        return value

    def _serialize_value(self, value: Any):
        if isinstance(value, datetime):
            return int(value.timestamp())
        return value

    def _remove_type_field_from_item(self, item: Dict[str, Any]):
        del item["type"]
        return item

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
        ]
        if len(hash_key_field) == 0 and model.type_is_primary_key():
            hash_key_field.append(model.type())
        hash_key_field = hash_key_field[0]
        sort_key_fields = [
            field_name
            for field_name, field_info in model_fields.items()
            if field_info.annotation is not None
            if field_info.annotation is IndexSecondaryKeyField and idx.name in getattr(model, field_name).index_names
        ]

        def _get_sort_key_value():
            if len(sort_key_fields) == 0 and model.include_type_in_sort_key():
                return model.type()
            if idx.sort_key.type is not str:
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

            sort_key_values: List[str] = []
            if model.include_type_in_sort_key() and model.type() not in sort_key_values:
                sort_key_values.append(model.type())

            for field in sort_key_fields:
                value = getattr(model, field).value
                sort_key_values.append(value)
            return self.delimiter.join(sort_key_values)

        def _get_hash_key_value():
            if hash_key_field == model.type():
                return model.type()
            else:
                return getattr(model, hash_key_field).value

        return {
            idx.hash_key.name: _get_hash_key_value(),
            idx.sort_key.name: _get_sort_key_value(),
        }

    def _perform_batch_write(self, put_items: List[DatabaseModel], delete_items: List[DatabaseModel]):
        if len(put_items) == 0 and len(delete_items) == 0:
            return

        dynamodb_table = self._get_dynamodb_table()

        if len(put_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in put_items:
                    data = self._serialize_item(item)
                    batch.put_item(Item=data)

        if len(delete_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in delete_items:
                    data = self._serialize_item(item)
                    batch.delete_item(Key=data)


class BatchWriteContext:
    def __init__(self, app: Table):
        self._table = app
        self._put_items: List[DatabaseModel] = []
        self._delete_items: List[DatabaseModel] = []

    def put(self, item: DatabaseModel):
        self._put_items.append(item)

    def delete(self, item: DatabaseModel):
        self._delete_items.append(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._table._perform_batch_write(self._put_items, self._delete_items)
