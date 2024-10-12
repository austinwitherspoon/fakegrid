"""Abstract base class for all backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from ..filters import ComplexFilterOperator, FilterOperator, SortDirection
from ..schema import Entity, Field, Schema


class BaseBackend(ABC):
    """Base class that all backends must inherit from."""

    def __init__(self, schema: Schema):
        self.schema = schema

    @abstractmethod
    def find(self, query: Query) -> list[dict[str, Any]]:
        """Find entities that match the query."""
        pass


@dataclass
class Query:
    """A query to run against the backend."""

    entity: Entity
    filters: ComplexFilter
    return_fields: list[FieldPath]
    order: list[QueryOrder]
    limit: int | None
    page: int | None
    retired_only: bool
    include_archived_projects: bool


@dataclass
class QueryFilter:
    """A simple query filter."""

    field: FieldPath
    operator: FilterOperator
    value: Any


@dataclass
class ComplexFilter:
    """A group of simple filters."""

    operator: ComplexFilterOperator
    filters: list[QueryFilter]


@dataclass
class FieldPath:
    """A path through multiple entities to get to a field."""

    pathway: list[FieldLinkedEntity] | None
    destination_field: Field


@dataclass
class FieldLinkedEntity:
    """A field, and the entity it points to."""

    field: Field
    destination_entity: Entity


@dataclass
class QueryOrder:
    """An order to apply to a query."""

    field: FieldPath
    direction: SortDirection
