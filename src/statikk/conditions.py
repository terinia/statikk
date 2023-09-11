from abc import ABC, abstractmethod
from typing import Any

from boto3.dynamodb.conditions import Key, ComparisonCondition


class Condition(ABC):
    def __init__(self, value: Any):
        self.value = value

    @abstractmethod
    def evaluate(self, key: Any) -> ComparisonCondition:
        pass


class Equals(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).eq(self.value)


class BeginsWith(Condition):
    def evaluate(self, key: Any) -> ComparisonCondition:
        return Key(key).begins_with(self.value)


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
