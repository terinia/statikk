"""
This module contains the Condition class and its subclasses. These classes are used to construct expressions understood
by DynamoDB to narrow down query results. Not to be confused with the ConditionExpression parameter, which is used to
narrow down filter results and is not implemented in this library. This library relies on the boto3 library to handle
the ConditionExpression filter_condition arguments.
See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Attr
for more information on filter expressions.

Example usage of this module:
    from statikk.conditions import Equals, BeginsWith
    app.query(range_key=Equals("123"), hash_key=BeginsWith("abc"))
"""

from abc import ABC, abstractmethod
from boto3.dynamodb.conditions import Key, ComparisonCondition
from typing import Any


class Condition(ABC):
    def __init__(self, value: Any):
        self.value = value

    @abstractmethod
    def evaluate(self, key: Any) -> ComparisonCondition:
        pass

    def enrich(self, **kwargs):
        pass


class Equals(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).eq(self.value)


class BeginsWith(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).begins_with(self.value)

    def enrich(self, model_class, **kwargs):
        if not self.value.startswith(model_class.type()):
            self.value = f"{model_class.type()}|{self.value}"


class LessThan(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).lt(self.value)


class GreaterThan(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).gt(self.value)


class LessThanOrEqual(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).lte(self.value)


class GreaterThanOrEqual(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).gte(self.value)


class Between(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).between(*self.value)
