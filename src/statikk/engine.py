import os
from datetime import datetime
from typing import Any, Dict, Type, Optional, List, Union

import boto3
from botocore.config import Config
from pydantic.fields import FieldInfo
from boto3.dynamodb.conditions import ComparisonCondition
from boto3.dynamodb.types import TypeDeserializer, Decimal

from statikk.typing import T, inspect_optional_field
from statikk.conditions import Condition, Equals, BeginsWith
from statikk.expressions import UpdateExpressionBuilder
from statikk.models import (
    DatabaseModel,
    GSI,
    KeySchema,
)
from statikk.fields import FIELD_STATIKK_TYPE, FIELD_STATIKK_PARENT_ID, FIELD_STATIKK_PARENT_FIELD_NAME
from copy import deepcopy
from aws_xray_sdk.core import patch_all

patch_all()


class InvalidIndexNameError(Exception):
    pass


class IncorrectSortKeyError(Exception):
    pass


class IncorrectHashKeyError(Exception):
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
                model.model_rebuild(force=True)
                model.set_table_ref(self)
        self._client = None
        self._dynamodb_table = None

    def _dynamodb_client(self):
        if self._client:
            return self._client

        self._client = boto3.client(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )

        return self._client

    def _get_dynamodb_table(self):
        if self._dynamodb_table:
            return self._dynamodb_table

        dynamodb = boto3.resource(
            "dynamodb",
            config=Config(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
        )
        self._dynamodb_table = dynamodb.Table(self.name)
        return self._dynamodb_table

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

    def _get_model_type_by_statikk_type(self, statikk_type: str) -> Type[DatabaseModel]:
        model_type_filter = [model_type for model_type in self.models if model_type.type() == statikk_type]
        if not model_type_filter:
            raise InvalidModelTypeError(
                f"Model type '{statikk_type}' not found. Make sure to register it through the models list."
            )
        return model_type_filter[0]

    def delete(self):
        """Deletes the DynamoDB table."""
        self._dynamodb_client().delete_table(TableName=self.name)

    def get_item(
        self,
        id: str,
        model_class: Type[T],
        sort_key: Optional[Any] = None,
        consistent_read: bool = False,
    ) -> T:
        """
        Returns an item from the database by id, using the partition key of the table.
        :param id: The id of the item to retrieve.
        :param model_class: The model class to use to deserialize the item.
        :param sort_key: The sort key of the item to retrieve. If the table does not have a sort key, this parameter should not be provided.
        """
        key = {self.key_schema.hash_key: id}
        if sort_key:
            key[self.key_schema.sort_key] = self._serialize_value(sort_key)
        raw_data = self._get_dynamodb_table().get_item(Key=key, ConsistentRead=consistent_read)
        if "Item" not in raw_data:
            raise ItemNotFoundError(f"{model_class} with id '{id}' not found.")
        data = raw_data["Item"]
        for key, value in data.items():
            if key == FIELD_STATIKK_TYPE:
                continue
            data[key] = self._deserialize_value(value, model_class.model_fields[key])
        return model_class(**data)

    def delete_item(self, model: DatabaseModel):
        """
        Deletes an item from the database by id, using the partition key of the table.
        :param id: The id of the item to delete.
        """
        with self.batch_write() as batch:
            for item in model.split_to_simple_objects():
                batch.delete(item)

    def put_item(self, model: DatabaseModel):
        """
        Puts an item into the database.

        This method automatically constructs values for the index fields of the provided model_class, based on the configured
        indexes on the table. If the model_class already has a value for the index field, the field's value is going to be used,
        otherwise the value is going to be constructed based on the model's fields. If a type field is not provided on the model class
        or the model instance, the type field is going to be set to the model class name. In case of string values, the values are
        always prefixed with the type of the model to avoid collisions between different model types.

        Example:
            class Card(DatabaseModelV2):
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

        with self.batch_write() as batch:
            items = model.split_to_simple_objects()
            new_keys = self._create_snapshot_representation(model)
            keys_to_delete = model._db_snapshot_keys - new_keys

            for key in keys_to_delete:
                hash_key, sort_key = key.split("#", 1)
                delete_params = {self.key_schema.hash_key: hash_key}
                if sort_key:
                    delete_params[self.key_schema.sort_key] = sort_key
                batch.delete_by_key(delete_params)

            for item in items:
                if item.should_delete:
                    batch.delete(item)
                else:
                    batch.put(item)

        model._db_snapshot_keys = new_keys

    def update_item(
        self,
        hash_key: str,
        update_builder: UpdateExpressionBuilder,
        model: DatabaseModel,
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

        def _find_changed_indexes():
            changed_index_values = set()
            for prefixed_attribute, value in expression_attribute_values.items():
                expression_attribute_values[prefixed_attribute] = self._serialize_value(value)
                attribute = prefixed_attribute.replace(":", "")
                for index_name, index_fields in model.index_definitions().items():
                    if attribute in index_fields.sk_fields:
                        setattr(model, attribute, value)
                        changed_index_values.add(index_name)

            return changed_index_values

        changed_index_values = _find_changed_indexes()

        for idx_name in changed_index_values:
            idx = [idx for idx in self.indexes if idx.name == idx_name][0]
            index_value = self._get_sort_key_value(model, idx)
            expression_attribute_values[f":{idx.sort_key.name}"] = index_value
            update_expression += f" SET {idx.sort_key.name} = :{idx.sort_key.name}"

        request = {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
            "ReturnValues": "ALL_NEW",
        }
        if expression_attribute_names:
            request["ExpressionAttributeNames"] = expression_attribute_names

        response = self._get_dynamodb_table().update_item(**request)
        data = response["Attributes"]
        for key, value in data.items():
            if key in [FIELD_STATIKK_TYPE, FIELD_STATIKK_PARENT_ID, FIELD_STATIKK_PARENT_FIELD_NAME]:
                continue
            data[key] = self._deserialize_value(value, model.model_fields[key])
        return type(model)(**data)

    def reparent_subtree(self, subtree_root: T, new_parent: T, field_name: str) -> T:
        subtree_root._parent = new_parent
        subtree_root._parent_field_name = field_name
        subtree_root.set_parent_references(subtree_root, force_override=True)
        parent = None
        for node in subtree_root.dfs_traverse_hierarchy():
            self.build_model_indexes(node)
            if parent:
                setattr(node, FIELD_STATIKK_PARENT_ID, parent.id)
            parent = node

        return subtree_root

    def build_model_indexes(self, model: T) -> T:
        for idx in self.indexes:
            new_index_values = self._compose_index_values(model, idx)
            new_sort_key_value = new_index_values[idx.sort_key.name]
            setattr(model, idx.sort_key.name, new_sort_key_value)
            new_hash_key_value = new_index_values[idx.hash_key.name]
            setattr(model, idx.hash_key.name, new_hash_key_value)
        return model

    def batch_write(self):
        """
        Returns a context manager for batch writing items to the database. This method handles all the buffering of the
        batch operation and the construction of index fields for each item.
        """
        return BatchWriteContext(self)

    def _prepare_index_query_params(
        self,
        hash_key: Union[Condition | str],
        model_class: Type[DatabaseModel],
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ):
        if isinstance(hash_key, str):
            hash_key = Equals(hash_key)
        if not index_name:
            index_name = self.indexes[0].name
        index_filter = [idx for idx in self.indexes if idx.name == index_name]
        if not index_filter:
            raise InvalidIndexNameError(f"The provided index name '{index_name}' is not configured on the table.")
        index = index_filter[0]
        key_condition = hash_key.evaluate(index.hash_key.name)
        if (
            not range_key or (range_key and range_key.value != model_class.type())
        ) and FIELD_STATIKK_TYPE not in model_class.index_definitions()[index_name].pk_fields:
            range_key = BeginsWith(model_class.type())
        if range_key:
            if not model_class.is_nested():
                range_key.enrich(model_class=model_class)
            key_condition = key_condition & range_key.evaluate(index.sort_key.name)

        query_params = {
            "IndexName": index_name,
            "KeyConditionExpression": key_condition,
        }

        if filter_condition:
            query_params["FilterExpression"] = filter_condition
        return query_params

    def query_index(
        self,
        hash_key: Union[Condition | str],
        model_class: Type[T],
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ) -> list[T]:
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
        query_params = self._prepare_index_query_params(
            hash_key=hash_key,
            model_class=model_class,
            range_key=range_key,
            filter_condition=filter_condition,
            index_name=index_name,
        )
        last_evaluated_key = True
        while last_evaluated_key:
            items = self._get_dynamodb_table().query(**query_params)
            yield from [model_class(**item) for item in items["Items"]]
            last_evaluated_key = items.get("LastEvaluatedKey", False)

    def query_hierarchy(
        self,
        hash_key: Union[Condition | str],
        model_class: Type[DatabaseModel],
        range_key: Optional[Condition] = None,
        filter_condition: Optional[ComparisonCondition] = None,
        index_name: Optional[str] = None,
    ) -> Optional[DatabaseModel]:
        query_params = self._prepare_index_query_params(
            hash_key=hash_key,
            model_class=model_class,
            range_key=range_key,
            filter_condition=filter_condition,
            index_name=index_name,
        )
        hierarchy_items = []
        last_evaluated_key = True
        while last_evaluated_key:
            items = self._get_dynamodb_table().query(**query_params)
            hierarchy_items.extend([item for item in items["Items"]])
            last_evaluated_key = items.get("LastEvaluatedKey", False)

        reconstructed_dict = self.reconstruct_hierarchy(hierarchy_items)

        if not reconstructed_dict:
            return None

        model_type = reconstructed_dict.get(FIELD_STATIKK_TYPE)
        model_class = self._get_model_type_by_statikk_type(model_type)

        reconstructed_dict.pop(FIELD_STATIKK_TYPE, None)
        model = model_class.model_validate(reconstructed_dict)
        model._db_snapshot_keys = self._create_snapshot_representation(model)
        return model

    def scan(
        self,
        filter_condition: Optional[ComparisonCondition] = None,
        consistent_read: bool = False,
    ) -> list[DatabaseModel]:
        """
        Scans the database for items matching the provided filter condition. The method returns a list of items matching
        the query, deserialized into the provided model_class parameter.

        :param model_class: The model class to use to deserialize the items.
        :param filter_condition: An optional filter condition to use for the query. See boto3.dynamodb.conditions.ComparisonCondition for more information.
        """
        query_params = {
            "ConsistentRead": consistent_read,
        }
        if filter_condition:
            query_params["FilterExpression"] = filter_condition
        last_evaluated_key = True

        while last_evaluated_key:
            items = self._get_dynamodb_table().scan(**query_params)
            yield from [self._get_model_type_by_statikk_type(item["__statikk_type"])(**item) for item in items["Items"]]
            last_evaluated_key = items.get("LastEvaluatedKey", False)

    def _convert_dynamodb_to_python(self, item) -> Dict[str, Any]:
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in item.items()}

    def batch_get_items(self, ids: List[str], model_class: Type[T], batch_size: int = 100) -> list[T]:
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
                            self._deserialize_item(self._convert_dynamodb_to_python(item), model_class)
                            for item in response["Responses"][self.name]
                        ]
                    )
                    break

        return results

    def _prepare_model_data(
        self,
        item: DatabaseModel,
        indexes: List[GSI],
    ) -> DatabaseModel:
        for idx in indexes:
            index_fields = self._compose_index_values(item, idx)
            for key, value in index_fields.items():
                if hasattr(item, key) and (getattr(item, key) is not None):
                    continue
                if value is not None:
                    setattr(item, key, value)
        item.model_rebuild(force=True)
        return item

    def _serialize_item(self, item: DatabaseModel):
        data = item.model_dump(exclude=item.get_nested_model_fields())
        serialized_data = {}
        for key, value in data.items():
            data[key] = self._serialize_value(value)
        return data

    def _deserialize_item(self, item: Dict[str, Any], model_class: Type[DatabaseModel]):
        for key, value in item.items():
            if key == FIELD_STATIKK_TYPE:
                continue
            item[key] = self._deserialize_value(value, model_class.model_fields[key])
        return model_class(**item)

    def _deserialize_value(self, value: Any, annotation: Any):
        actual_annotation = annotation.annotation if hasattr(annotation, "annotation") else annotation

        if actual_annotation is datetime or "datetime" in str(annotation) and value is not None:
            return datetime.fromtimestamp(int(value))
        if actual_annotation is float:
            return float(value)
        if actual_annotation is list:
            origin = getattr(actual_annotation, "__origin__", None)
            args = getattr(actual_annotation, "__args__", None)
            item_annotation = args[0] if args else Any
            return [self._deserialize_value(item, item_annotation) for item in value]
        if actual_annotation is set:
            origin = getattr(actual_annotation, "__origin__", None)
            args = getattr(actual_annotation, "__args__", None)
            item_annotation = args[0] if args else Any
            return {self._deserialize_value(item, item_annotation) for item in value}
        if isinstance(value, dict):
            return {key: self._deserialize_value(item, annotation) for key, item in value.items() if item is not None}
        return value

    def _serialize_value(self, value: Any):
        if isinstance(value, datetime):
            return int(value.timestamp())
        if isinstance(value, float):
            return Decimal(value)
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, set):
            return {self._serialize_value(item) for item in value}
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items() if item is not None}
        return value

    def _set_index_fields(self, model: Type[DatabaseModel], idx: GSI):
        model_fields = model.model_fields
        if idx.hash_key.name not in model_fields.keys():
            model_fields[idx.hash_key.name] = FieldInfo(annotation=idx.hash_key.type, default=None, required=False)
        if idx.sort_key.name not in model_fields.keys():
            model_fields[idx.sort_key.name] = FieldInfo(annotation=idx.sort_key.type, default=None, required=False)

    def _get_sort_key_value(self, model: DatabaseModel, idx: GSI) -> str:
        sort_key_fields = model.index_definitions().get(idx.name, []).sk_fields
        if len(sort_key_fields) == 0:
            raise IncorrectSortKeyError(f"Model {model.__class__} does not have a sort key defined.")

        if idx.sort_key.type is not str:
            value = getattr(model, sort_key_fields[0])
            if type(value) is not idx.sort_key.type:
                raise IncorrectSortKeyError(
                    f"Incorrect sort key type. Sort key type for sort key '{idx.sort_key.name}' should be: "
                    + str(idx.sort_key.type)
                    + " but got: "
                    + str(type(value))
                )
            value = value or idx.sort_key.default
            return self._serialize_value(value)

        sort_key_values = []
        if model._parent:
            sort_key_values.append(getattr(model._parent, idx.sort_key.name))
        if FIELD_STATIKK_TYPE not in model.index_definitions()[idx.name].pk_fields:
            sort_key_values.append(model.type())
        for sort_key_field in sort_key_fields:
            if sort_key_field in model.model_fields.keys():
                sort_key_values.append(self._serialize_value(getattr(model, sort_key_field)))

        return self.delimiter.join(sort_key_values)

    def _compose_index_values(self, model: DatabaseModel, idx: GSI) -> Dict[str, Any]:
        hash_key_fields = model.index_definitions().get(idx.name, None)
        if hash_key_fields is None:
            raise IncorrectHashKeyError(f"Model {model.__class__} does not have a hash key defined.")

        hash_key_fields = hash_key_fields.pk_fields
        if len(hash_key_fields) == 0 and not model.is_nested():
            raise IncorrectHashKeyError(f"Model {model.__class__} does not have a hash key defined.")

        def _get_hash_key_value():
            if model._parent:
                return getattr(model._parent, idx.hash_key.name)

            return self.delimiter.join([self._serialize_value(model.get_attribute(field)) for field in hash_key_fields])

        return {
            idx.hash_key.name: _get_hash_key_value(),
            idx.sort_key.name: self._get_sort_key_value(model, idx),
        }

    def _perform_batch_write(
        self, put_items: List[DatabaseModel], delete_items: List[DatabaseModel], delete_keys: list[dict[str, Any]]
    ):
        if len(put_items) == 0 and len(delete_items) == 0:
            return

        dynamodb_table = self._get_dynamodb_table()

        if len(delete_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in delete_items:
                    enriched_item = self._prepare_model_data(item, self.indexes)
                    data = self._serialize_item(enriched_item)
                    batch.delete_item(Key=data)

        if len(delete_keys) > 0:
            with dynamodb_table.batch_writer() as batch:
                for key in delete_keys:
                    batch.delete_item(Key=key)

        if len(put_items) > 0:
            with dynamodb_table.batch_writer() as batch:
                for item in put_items:
                    enriched_item = self._prepare_model_data(item, self.indexes)
                    if not enriched_item.was_modified:
                        continue
                    if not enriched_item.should_write_to_database():
                        continue
                    data = self._serialize_item(enriched_item)
                    batch.put_item(Item=data)

    def _create_snapshot_representation(self, model: DatabaseModel) -> set:
        db_snasphot = set()
        for node in model.dfs_traverse_hierarchy():
            model_key = {self.key_schema.hash_key: getattr(node, self.key_schema.hash_key)}
            if self.key_schema.sort_key:
                model_key[self.key_schema.sort_key] = getattr(node, self.key_schema.sort_key)
            key_string = f"{model_key.get(self.key_schema.hash_key)}#{model_key.get(self.key_schema.sort_key, '')}"
            db_snasphot.add(key_string)
        return db_snasphot

    def reconstruct_hierarchy(self, items: list[dict]) -> Optional[dict]:
        """
        Reconstructs a hierarchical dictionary structure from a flat list of dictionaries
        using explicit parent-child relationships and model class definitions.

        Args:
            items: A flat list of dictionaries representing models with FIELD_STATIKK_PARENT_ID

        Returns:
            The top-level dictionary with its hierarchy fully reconstructed, or None if the list is empty
        """
        items_by_id = {item["id"]: item for item in items}
        children_by_parent_id = {}
        for item in items:
            parent_id = item.get(FIELD_STATIKK_PARENT_ID)
            if parent_id:
                if parent_id not in children_by_parent_id:
                    children_by_parent_id[parent_id] = []
                children_by_parent_id[parent_id].append(item)

        root_items = [item for item in items if FIELD_STATIKK_PARENT_ID not in item]
        if not root_items:
            return None

        root_item = root_items[0]
        processed_root = self._process_item(root_item, items_by_id, children_by_parent_id)
        return processed_root

    def _process_item(self, item: dict, items_by_id: dict, children_by_parent_id: dict) -> dict:
        """
        Recursively processes an item and all its children to rebuild the hierarchical structure.

        Args:
            item: The current item to process
            items_by_id: Dictionary mapping item IDs to items
            children_by_parent_id: Dictionary mapping parent IDs to lists of child items

        Returns:
            The processed item with all its child relationships resolved
        """
        processed_item = deepcopy(item)

        if FIELD_STATIKK_TYPE in processed_item:
            model_class = self._get_model_type_by_statikk_type(processed_item[FIELD_STATIKK_TYPE])
            model_fields = model_class.model_fields
        else:
            return processed_item

        # Get children of this item
        children = children_by_parent_id.get(processed_item["id"], [])

        # Group children by parent field name
        children_by_field = {}
        for child in children:
            field_name = child.get(FIELD_STATIKK_PARENT_FIELD_NAME)
            if field_name:
                if field_name not in children_by_field:
                    children_by_field[field_name] = []
                children_by_field[field_name].append(child)

        for field_name, field_info in model_fields.items():
            if field_name not in children_by_field:
                continue

            field_children = children_by_field[field_name]

            field_type = field_info.annotation
            is_optional = False
            inner_type = field_type

            if hasattr(field_type, "__origin__") and field_type.__origin__ is Union:
                args = field_type.__args__
                if type(None) in args:
                    is_optional = True
                    # Get the non-None type
                    inner_type = next(arg for arg in args if arg is not type(None))

            if hasattr(inner_type, "__origin__") and inner_type.__origin__ == list:
                child_list = []

                for child in field_children:
                    processed_child = self._process_item(child, items_by_id, children_by_parent_id)
                    child_list.append(processed_child)

                processed_item[field_name] = child_list

            elif len(field_children) == 1:
                processed_child = self._process_item(field_children[0], items_by_id, children_by_parent_id)
                processed_item[field_name] = processed_child

        return processed_item


class BatchWriteContext:
    def __init__(self, app: Table):
        self._table = app
        self._put_items: List[DatabaseModel] = []
        self._delete_items: List[DatabaseModel] = []
        self._delete_keys: List[dict[str, Any]] = []

    def put(self, item: DatabaseModel):
        self._put_items.append(item)

    def delete(self, item: DatabaseModel):
        self._delete_items.append(item)

    def delete_by_key(self, key: dict[str, Any]):
        self._delete_keys.append(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._table._perform_batch_write(self._put_items, self._delete_items, self._delete_keys)
