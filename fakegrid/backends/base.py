"""Abstract base class for all backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from ..filters import (
    ComplexFilter,
    ComplexFilterOperator,
    Filter,
    FilterOperator,
    SortDirection,
)
from ..schema import Entity, Field, Schema


class BaseBackend(ABC):
    """Base class that all backends must inherit from."""

    def __init__(self, schema: Schema):
        self.schema = schema

    @abstractmethod
    def find(self, query: BackendQuery) -> list[dict[str, Any]]:
        """Find entities that match the query."""
        raise NotImplementedError


@dataclass
class BackendQuery:
    """A query to run against the backend."""

    entity: Entity
    filters: BackendComplexFilter
    return_fields: list[FieldPath]
    order: list[BackendQueryOrder]
    limit: int | None
    page: int | None
    retired_only: bool
    include_archived_projects: bool


@dataclass
class BackendFilter:
    """A simple query filter."""

    field: FieldPath
    operator: FilterOperator
    value: Any

    @classmethod
    def from_filter(cls, base_entity: Entity, filter_: Filter) -> BackendFilter:
        """Adapts a user-facing filter to a more defined filter that the backend can understand."""
        return BackendFilter(
            field=FieldPath.from_string(base_entity, filter_.field),
            operator=filter_.operator,
            value=filter_.value,
        )


@dataclass
class BackendComplexFilter:
    """A group of simple filters."""

    operator: ComplexFilterOperator
    filters: list[BackendFilter]

    @classmethod
    def from_filter(cls, base_entity: Entity, filter_: Filter | ComplexFilter) -> BackendComplexFilter:
        """Adapts a user-facing filter to a more defined filter that the backend can understand."""
        if isinstance(filter_, Filter):
            return BackendComplexFilter(
                operator=ComplexFilterOperator.ALL,
                filters=[BackendFilter.from_filter(base_entity, filter_)],
            )

        return BackendComplexFilter(
            operator=filter_.operator,
            filters=[BackendFilter.from_filter(base_entity, f) for f in filter_.filters],
        )


@dataclass
class FieldPath:
    """A path through multiple entities to get to a field."""

    pathway: list[FieldLinkedEntity] | None
    destination_field: Field

    @classmethod
    def from_string(cls, base_entity: Entity, field_path: str) -> FieldPath:
        """Solve the actual path to a field from a string.

        The input is going to be field_name.EntityType.field_name.EntityType.field_name... etc.
        Always ending with a field name, so there should always be an odd number of elements.
        """
        schema = base_entity.schema
        field_parts = field_path.split(".")

        current_field_name = field_parts.pop(0)
        current_field = base_entity[current_field_name]

        pathway: list[FieldLinkedEntity] = []
        while field_parts:
            entity_name = field_parts.pop(0)
            entity = next(iter(e for e in schema.entities if e.api_name == entity_name), None)

            if entity is None:
                raise ValueError(f"Entity {entity_name} not found in schema")

            pathway.append(
                FieldLinkedEntity(
                    field=current_field,
                    destination_entity=entity,
                )
            )
            current_field_name = field_parts.pop(0)
            current_field = entity[current_field_name]

        return FieldPath(
            pathway=pathway,
            destination_field=current_field,
        )


@dataclass
class FieldLinkedEntity:
    """A field, and the entity it points to."""

    field: Field
    destination_entity: Entity


@dataclass
class BackendQueryOrder:
    """An order to apply to a query."""

    field: FieldPath
    direction: SortDirection
