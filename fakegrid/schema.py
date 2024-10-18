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

    @classmethod
    def from_json(
        cls,
        schema_read: dict[str, Any],
        schema_entity_read: dict[str, Any],
        javascript_schema: dict[str, Any] | None,
    ) -> Schema:
        """Build the site schema from the json returned by both schema endpoints.

        Javascript schema (from the /reload_schema endpoint used by the website) is important
        because it contains reverse field metadata the `sg.schema_read()` does not. If you don't
        provide this, corresponding fields won't be updated (when you update a user's projects, the
        project's users field won't be updated!).
        """

        schema = cls(entities=[])

        for entity_name, fields in schema_read.items():
            if entity_name in IGNORE_ENTITIES:
                continue
            metadata = schema_entity_read[entity_name]
            schema.entities.append(Entity.from_json(schema, entity_name, metadata, fields))

        if javascript_schema:
            cls._resolve_links(schema, javascript_schema)

        return schema

    def entity_map(self) -> dict[str, Entity]:
        """Get a map of entity names to entities."""
        if self._entity_map is None:
            self._entity_map = {entity.api_name: entity for entity in self.entities}
        return self._entity_map

    def __contains__(self, entity_name: str) -> bool:
        """Check if an entity is in the schema."""
        return entity_name in self.entity_map()

    def __getitem__(self, entity_name: str) -> Entity:
        """Get an entity by name."""
        entity = self.entity_map().get(entity_name)
        if entity:
            return entity
        raise ValueError(f"Entity {entity_name} not found in schema.")

    def get(self, entity_name: str) -> Entity | None:
        """Get an entity by name."""
        return self.entity_map().get(entity_name)

    def add(self, entity: Entity) -> None:
        """Add an entity to the schema."""
        self.entities.append(entity)
        self._entity_map = None

    @staticmethod
    def _resolve_links(schema: Schema, javascript_schema: dict[str, Any]) -> None:
        """Resolve the links between fields in the schema."""

        js_metadata = javascript_schema["schema"]["entity_fields"]

        # make sure we process Connection entities first, so links are resolved in those before we
        # do normal entities!
        entities = sorted(schema.entities, key=lambda e: not e.api_name.endswith("Connection"))

        for entity in entities:
            js_entity = js_metadata[entity.api_name]
            is_connection_entity = entity.api_name.endswith("Connection")

            for field in entity.fields:  # noqa: F402
                if field.field_type not in LINK_FIELDS:
                    continue

                field_properties = js_entity[field.api_name]

                if is_connection_entity:
                    Schema._resolve_connection_entity_link(field, field_properties)
                    continue

                reverse_of = field_properties.get("reverse_of")
                inverse_association = field_properties.get("inverse_association")
                join_table = field_properties.get("through_join_entity_type")

                if reverse_of:
                    reverse_entity_name = reverse_of["entity_type_name"]
                    reverse_field_name = reverse_of["name"]
                elif isinstance(inverse_association, str):
                    reverse_entity_name, reverse_field_name = inverse_association.split(".")
                elif join_table:
                    # This is a weird situation where there's no actual connection table
                    # but the javascript schema refers to one. Let's make it!
                    Schema._resolve_multi_entity_link(field, field_properties, None)
                    continue

                # if NONE of those fields exist at all, this field doesn't link to anything
                if not any([reverse_of, inverse_association, join_table]):
                    # but we still want to set up a connection table if it's a multi-entity field
                    if field.field_type == FieldType.MULTI_ENTITY:
                        Schema._resolve_multi_entity_link(field, field_properties, None)
                    continue

                linked_entity = schema.get(reverse_entity_name)
                linked_field = (linked_entity or {}).get(reverse_field_name)

                if linked_field and linked_field.field_type == FieldType.ENTITY:
                    Schema._resolve_single_entity_link(field, linked_field)
                    continue

                if (
                    field.field_type == FieldType.MULTI_ENTITY
                    and linked_field
                    and linked_field.field_type == FieldType.MULTI_ENTITY
                ):
                    Schema._resolve_multi_entity_link(field, field_properties, linked_field)
                    continue

    @staticmethod
    def _resolve_single_entity_link(field: Field, linked_field: Field) -> None:
        """Resolve a single entity link between two fields."""
        field.link = field.link or OneToManyLink(parent=field, children=[])
        assert isinstance(field.link, OneToManyLink)
        field.link.children.append(linked_field)
        linked_field.link = field.link

    @staticmethod
    def _resolve_multi_entity_link(
        field: Field, field_js_schema: dict[str, Any] | None, linked_field: Field | None
    ) -> None:
        """Resolve a multi entity link between two fields."""
        schema_connection_entity_name = (field_js_schema or {}).get("through_join_entity_type")

        # Field on the connection entity that links back to this field
        connection_entity_field = None

        # Get the connection entity from the schema if available
        if schema_connection_entity_name and schema_connection_entity_name in field.entity.schema:
            connection_entity = field.entity.schema[schema_connection_entity_name]
            # in this case, the connection entity should already be linked to this field!
            connection_entity_field = next(
                (
                    f
                    for f in connection_entity.fields
                    if isinstance(f.link, ConnectionEntityLink) and f.link.linked == field
                ),
            )

        # Otherwise find it on the linked field, if available
        elif linked_field and isinstance(linked_field.link, ManyToManyLink):
            connection_entity = linked_field.link.connection_entity
            # add our field to the connection entity
            connection_field_name = get_next_available_connection_entity_field_name(connection_entity, field)
            connection_entity_field = Field(
                entity=connection_entity,
                api_name=connection_field_name,
                display_name=snake_case_to_display(connection_field_name),
                field_type=FieldType.ENTITY,
                metadata={},
            )
            connection_entity.add(connection_entity_field)

        # Otherwise create a new connection entity
        else:
            linked_entity_name = linked_field.entity.api_name if linked_field else None
            if not linked_entity_name:
                try:
                    linked_entity_name = (
                        field.metadata.get("properties", {}).get("valid_types", {}).get("value", ["Unknown"])[0]
                    )
                except IndexError:
                    return
            api_name = schema_connection_entity_name or f"Fake{field.entity.api_name}{linked_entity_name}Connection"
            display_name = camel_case_to_display(api_name)
            connection_entity = Entity(
                schema=field.entity.schema,
                api_name=api_name,
                display_name=display_name,
                fields=[],
            )
            field.entity.schema.add(connection_entity)

            connection_field_name = get_next_available_connection_entity_field_name(connection_entity, field)
            connection_entity_field = Field(
                entity=connection_entity,
                api_name=connection_field_name,
                display_name=snake_case_to_display(connection_field_name),
                field_type=FieldType.ENTITY,
                metadata={},
            )
            connection_entity.add(connection_entity_field)

        # now that we have a connection entity, update the link
        field.link = field.link or ManyToManyLink(
            connection_entity=connection_entity,
            field_to_connection_entity_field={},
        )
        assert isinstance(field.link, ManyToManyLink)

        field.link.field_to_connection_entity_field[field] = connection_entity_field

    @staticmethod
    def _resolve_connection_entity_link(field: Field, field_properties: dict[str, Any]) -> None:
        """Resolve a link on a connection entity."""
        try:
            linked_entity, linked_field = field_properties["inverse_association"].split(".")
        except KeyError:
            # if "inverse_association" is missing, this field doesn't get exposed on the other entity!
            return
        try:
            linked_entity = field.entity.schema[linked_entity]
        except ValueError:
            # Some connection entities link to entities that don't exist in the schema (?!)
            # We need to remove these from the schema or things break
            schema = field.entity.schema
            schema.entities = [e for e in schema.entities if e.api_name != field.entity.api_name]
            schema._entity_map = None
            return None

        linked_field = linked_entity[linked_field]

        field.link = ConnectionEntityLink(linked=linked_field)


@dataclass
class Entity:
    """An entity/table in shotgrid."""

    schema: Schema = field(repr=False)
    api_name: str
    display_name: str
    fields: list[Field]
    visible: bool = True

    _field_map: dict[str, Field] | None = field(init=False, repr=False, default=None)

    def field_map(self) -> dict[str, Field]:
        """Get a map of field names to fields."""
        if self._field_map is None:
            self._field_map = {field.api_name: field for field in self.fields}
        return self._field_map

    def __contains__(self, field_name: str) -> bool:
        """Check if a field is in the entity."""
        return field_name in self.field_map()

    def __getitem__(self, field_name: str) -> Field:
        """Get a field by name."""
        field = self.field_map().get(field_name)
        if field:
            return field
        raise ValueError(f"Field {field_name} not found in entity {self.api_name}")

    def get(self, field_name: str) -> Field | None:
        """Get a field by name."""
        return self.field_map().get(field_name)

    def add(self, field: Field) -> None:
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
            entity.add(Field.from_json(entity, field_name, field_data))

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

    link: OneToManyLink | ManyToManyLink | ConnectionEntityLink | None = None

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

    def valid_types(self) -> list[Entity]:
        """Get the entities that this field can link to."""
        if self.field_type not in LINK_FIELDS:
            raise ValueError(f"Field {self.api_name} is not a link field.")
        entity_names = self.metadata.get("properties", {}).get("valid_types", {}).get("value", [])
        entities = []
        for entity_name in entity_names:
            entity = self.entity.schema.get(entity_name)
            if entity:
                entities.append(entity)
        return entities

    def __hash__(self) -> int:
        return hash(f"{self.entity.api_name}.{self.api_name}")


@dataclass
class ConnectionEntityLink:
    """A Link to the parent field, on a connection table."""

    linked: Field


@dataclass
class OneToManyLink:
    """A link between two fields."""

    parent: Field
    children: list[Field]


@dataclass
class ManyToManyLink:
    connection_entity: Entity
    field_to_connection_entity_field: dict[Field, Field]


def get_next_available_connection_entity_field_name(connection_entity: Entity, linked_field: Field) -> str:
    """Get the next available name for a field on a connection entity."""

    proposed_name = f"{camel_case_to_snake_case(linked_field.entity.api_name)}"

    if proposed_name not in connection_entity.field_map():
        return proposed_name

    for i in range(1, 100):
        if f"{proposed_name}_{i}" not in connection_entity.field_map():
            return f"{proposed_name}_{i}"

    raise ValueError("Unable to find a unique name for the connection entity field.")


def camel_case_to_snake_case(name: str) -> str:
    """Convert a camel case name to snake case."""
    return "".join(["_" + c.lower() if c.isupper() else c for c in name]).lstrip("_")


def snake_case_to_display(name: str) -> str:
    """Convert a snake case name to a display name."""
    return " ".join(name.split("_")).title()


def camel_case_to_display(name: str) -> str:
    """Convert a camel case name to a display name."""
    return snake_case_to_display(camel_case_to_snake_case(name))
