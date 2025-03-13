from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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


LINK_FIELDS = [
    FieldType.MULTI_ENTITY,
    FieldType.ENTITY,
    FieldType.ADDRESSING,
    FieldType.TAG_LIST,
]

IGNORED_TYPES = [
    FieldType.PIVOT_COLUMN,
    FieldType.CALCULATED,
    FieldType.SUMMARY,
    FieldType.JSON_B,
]

IGNORE_ENTITIES = [
    "AppWelcome",  # this shows up in linked field, but doesn't exist?
    "AppWelcomeUserConnection",  # Broken because of the above
    "Banner",  # this shows up in linked field, but doesn't exist?
    "BannerUserConnection",  # Broken because of the above
]


@dataclass
class Schema:
    """The schema of an entire shotgrid site."""

    entities: list[Entity]

    _entity_map: dict[str, Entity] | None = field(init=False, repr=False, default=None)

    def __entity_map__(self) -> dict[str, Entity]:
        """Get a map of entity names to entities."""
        if self._entity_map is None:
            self._entity_map = {entity.api_name: entity for entity in self.entities}
        return self._entity_map

    def get_entity(self, entity_name: str) -> Entity | None:
        """Get an entity by name."""
        return self.__entity_map__().get(entity_name)

    def __getitem__(self, entity_name: str) -> Entity:
        """Get an entity by name."""
        entity = self.get_entity(entity_name)
        if entity:
            return entity
        raise ValueError(f"Entity {entity_name} not found in schema")

    def add_entity(self, entity: Entity) -> None:
        """Add an entity to the schema."""
        self.entities.append(entity)
        self._entity_map = None


@dataclass
class Entity:
    """An entity/table in shotgrid."""

    schema: Schema = field(repr=False)
    api_name: str
    display_name: str
    fields: list[Field]
    visible: bool = True

    _field_map: dict[str, Field] | None = field(init=False, repr=False, default=None)

    def __field_map__(self) -> dict[str, Field]:
        """Get a map of field names to fields."""
        if self._field_map is None:
            self._field_map = {field.api_name: field for field in self.fields}
        return self._field_map

    def __contains__(self, field_name: str) -> bool:
        """Check if a field is in the entity."""
        return field_name in self.__field_map__()

    def __getitem__(self, field_name: str) -> Field:
        """Get a field by name."""
        field = self.__field_map__().get(field_name)
        if field:
            return field
        raise ValueError(f"Field {field_name} not found in entity {self.api_name}")

    def get_or_create_field(self, field_name: str, field_type: FieldType) -> Field:
        """Get a field by name, or create it if it doesn't exist."""
        field = self.get_field(field_name)
        if field:
            return field
        field = Field(
            entity=self,
            api_name=field_name,
            display_name=field_name,
            field_type=field_type,
            metadata={},
        )
        self.add_field(field)
        return field

    def get_field(self, field_name: str) -> Field | None:
        """Get a field by name."""
        return self.__field_map__().get(field_name)

    def add_field(self, field: Field) -> None:
        """Add a field to the entity."""
        self.fields.append(field)
        self._field_map = None

    @classmethod
    def from_json(
        cls,
        schema: Schema,
        api_name: str,
        metadata: dict[str, Any],
        fields: dict[str, Any],
    ) -> Entity:
        """Build an entity from the json returned by the schema endpoint."""
        entity = cls(
            schema=schema,
            api_name=api_name,
            display_name=metadata["name"]["value"],
            fields=[],
            visible=metadata["visible"]["value"],
        )

        for field_name, field_data in fields.items():
            entity.add_field(Field.from_json(entity, field_name, field_data))

        return entity


@dataclass
class Field:
    """The metadata that represents a Shotgrid field."""

    entity: Entity = field(repr=False)
    api_name: str
    display_name: str
    field_type: FieldType
    metadata: dict[str, Any]
    mandatory: bool = False
    editable: bool = True
    unique: bool = False

    link: Link | None = None

    # store extra metadata needed for parsing the schema
    _js_schema: Any | None = field(init=False, repr=False, default=None)

    @classmethod
    def from_json(cls, entity: Entity, api_name: str, field_data: dict[str, Any]) -> Field:
        """Build a field from the json returned by the schema endpoint."""
        field_type = FieldType(field_data["data_type"]["value"])
        display_name = field_data["name"]["value"]
        mandatory = field_data["mandatory"]["value"]
        editable = field_data["editable"]["value"]
        unique = field_data["unique"]["value"]

        return cls(
            entity=entity,
            api_name=api_name,
            display_name=display_name,
            field_type=field_type,
            mandatory=mandatory,
            editable=editable,
            unique=unique,
            metadata=field_data,
            link=None,
        )

    def __hash__(self) -> int:
        return hash(f"{self.entity.api_name}.{self.api_name}")


@dataclass
class SingleEntityLink:
    """A field that links to a single other entity."""

    children: list[Field]


@dataclass
class ReverseOfSingleEntityLink:
    """A field that pulls it's data from a single entity link field.

    This will return multiple entities, any that match the id on the parent field.
    """

    parent: Field


@dataclass
class MultiEntityLink:
    """A field that links to multiple other entities."""

    connection_entity: Entity
    field_to_connection_entity_field: tuple[Field, Field]


Link = SingleEntityLink | ReverseOfSingleEntityLink | MultiEntityLink
