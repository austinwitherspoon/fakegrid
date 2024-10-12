from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FieldType(Enum):
    """Shotgrid field types."""

    TEXT = "text"
    FLOAT = "float"
    MULTI_ENTITY = "multi_entity"
    NUMBER = "number"
    ADDRESSING = "addressing"
    CHECKBOX = "checkbox"
    COLOR = "color"
    CURRENCY = "currency"
    DATE = "date"
    DATE_TIME = "date_time"
    DURATION = "duration"
    ENTITY = "entity"
    FOOTAGE = "footage"
    IMAGE = "image"
    LIST = "list"
    PASSWORD = "password"  # noqa: S105
    PERCENT = "percent"
    SERIALIZABLE = "serializable"
    STATUS_LIST = "status_list"
    SUMMARY = "summary"
    TAG_LIST = "tag_list"
    TIMECODE = "timecode"
    URL = "url"
    ENTITY_TYPE = "entity_type"
    PIVOT_COLUMN = "pivot_column"
    UUID = "uuid"
    JSON_B = "jsonb"
    CALCULATED = "calculated"


IGNORED_TYPES = [
    FieldType.PIVOT_COLUMN,
    FieldType.CALCULATED,
    FieldType.SUMMARY,
    FieldType.JSON_B,
]


@dataclass
class Schema:
    """The schema of an entire shotgrid site."""

    entities: list[Entity]


@dataclass
class Entity:
    """An entity/table in shotgrid."""

    schema: Schema
    api_name: str
    display_name: str
    fields: list[Field]

    def get_field(self, field_name: str) -> Field:
        """Get a field by name."""
        for field in self.fields:
            if field.api_name == field_name:
                return field
        raise ValueError(f"Field {field_name} not found in entity {self.api_name}")


@dataclass
class Field:
    """The metadata that represents a Shotgrid field."""

    entity: Entity
    api_name: str
    display_name: str
    field_type: FieldType

    link: FieldLink | None

    def reverse_field(self, target_entity_type: str) -> Field | None:
        """Get the field that links back to this field."""
        if self.link is None:
            return None
        if self.link.parent is not self:
            return self.link.parent
        for child in self.link.children:
            if child.entity.api_name == target_entity_type:
                return child
        return None


@dataclass
class FieldLink:
    """A link between two fields."""

    parent: Field
    children: list[Field]
    connection_entity: Entity | None
