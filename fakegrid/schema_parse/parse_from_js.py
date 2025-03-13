"""Parse the Shotgun JS API schema data and build our fakegrid schema.


What we know:
- `reverse_fields` is filled in on a single entity link, showing all reverse fields.
- multi-entity links USUALLY have a through_join_field and through_join_entity_type.
- SOMETIMES inverse_association or reverse_of is filled in on either a multi or single entity link
- `primary_side_of` is sometimes filled in on multi entity links, similar to `inverse_association` or `reverse_of`

"""

from fakegrid.schema import (
    Entity,
    Field,
    FieldType,
    MultiEntityLink,
    ReverseOfSingleEntityLink,
    Schema,
    SingleEntityLink,
)

from .js_schema import JsSchema, JsSchemaEntityFieldData

IGNORE_CONNECTION_ENTITIES = {
    "ActionMenuItemProjectConnection",  # doesn't exist
    "ActionMenuItemPermissionConnection",  # doesn't exist
    "ProjectSoftwareConnection",  # doesn't exist
    "TankActionPermissionRuleSetDenial",  # doesn't exist
}

IGNORED_FIELD_NAMES = {"open_notes", "sibling_tasks"}

CREATE_TABLES = {"Addressing", "AttachmentLink"}

# Connections we can infer exist by looking at the schema but don't actually exist
MISSING_CONNECTIONS = {
    ("Note", "note_links"): ("NoteLink", "note"),
    ("Task", "notes"): ("NoteTask", "task"),
    ("Task", "task_assignees"): ("Addressing", "user"),
    ("Task", "task_reviewers"): ("Addressing", "user"),  # TODO: ???
    ("*", "notes"): ("NoteLink", "entity"),
    ("*", "tags"): ("Tagging", "entity"),
    ("*", "addressings_cc"): ("Addressing", "entity"),
    ("*", "addressings_to"): ("Addressing", "entity"),  # TODO: ???
    ("*", "attachments"): ("AttachmentLink", "entity"),
}


# TODO: Version.tasks is unlinked but metadata is there!

# Currently broken:
# Task:
#   - sibling_tasks
# Note:
#   - replies
# Version:
#   - tasks
# Attachment:
#   - attachment_reference_links
#   - attachment_links
# Ticket:
#   - replies
# Revision:
#   - published_files
# Delivery:
#   - replies
# ActionMenuItem:
#   - projects
#   - permissions_groups
# Software:
#   - projects
#   - user_restrictions
# Composition:
#   - composition_links
# PublishEvent:
#   - publish_event_links
# Launch:
#   - tasks
# TankContainer:
#   - tank_container_links
# TankAction:
#   - deny_permissions


def build_fakegrid_schema(raw_schema: JsSchema) -> Schema:
    """Use the raw schema data to build a fakegrid schema."""
    schema = Schema(entities=[])

    for entity_name, field_data in raw_schema.entity_fields.items():
        entity = Entity(
            schema=schema,
            api_name=entity_name,
            display_name=entity_name,  # TODO: get the display name from the schema
            fields=[],
            visible=True,
        )
        schema.add_entity(entity)

        for field_name, js_field_data in field_data.items():
            field = Field(
                entity=entity,
                api_name=field_name,
                display_name=js_field_data.display_name,
                field_type=FieldType(js_field_data.data_type),
                metadata={},
                mandatory=False,
                editable=True,
                unique=False,
            )
            field._js_schema = js_field_data
            entity.add_field(field)

    for table_name in CREATE_TABLES:
        if not schema.get_entity(table_name):
            entity = Entity(
                schema=schema,
                api_name=table_name,
                display_name=table_name,
                fields=[],
                visible=True,
            )
            schema.add_entity(entity)

    resolve_multi_entity_links(schema)
    resolve_single_entity_links(schema)

    return schema


def resolve_multi_entity_links(schema: Schema) -> None:
    """Resolve multi-entity links."""
    for entity in schema.entities:
        for field in entity.fields:
            if field.field_type != FieldType.MULTI_ENTITY:
                continue
            if field.api_name in IGNORED_FIELD_NAMES:
                continue
            raw_field_data: JsSchemaEntityFieldData = field._js_schema  # type: ignore

            default_connection = next(
                (
                    dest
                    for source, dest in MISSING_CONNECTIONS.items()
                    if source[0] in [entity.api_name, "*"] and source[1] == field.api_name
                ),
                None,
            )
            if (raw_field_data.through_join_field and raw_field_data.through_join_entity_type) or default_connection:
                if raw_field_data.through_join_entity_type in IGNORE_CONNECTION_ENTITIES:
                    continue
                connection_entity = (
                    schema.get_entity(raw_field_data.through_join_entity_type)
                    if raw_field_data.through_join_entity_type
                    else None
                )
                connection_field = (
                    next(
                        (
                            f
                            for f in connection_entity.fields
                            if f._js_schema
                            and f._js_schema.inverse_association == f"{entity.api_name}.{field.api_name}"
                        ),
                        None,
                    )
                    if connection_entity
                    else None
                )
                if not connection_field and default_connection:
                    connection_entity_name, connection_field_name = default_connection
                    connection_entity = schema.get_entity(connection_entity_name)
                    assert connection_entity is not None
                    connection_field = connection_entity.get_or_create_field(connection_field_name, FieldType.ENTITY)

                assert connection_entity is not None
                assert connection_field is not None
                field.link = MultiEntityLink(connection_entity, (field, connection_field))


def resolve_single_entity_links(schema: Schema) -> None:
    """Resolve single-entity links."""

    for entity in schema.entities:
        for field in entity.fields:
            if field.field_type != FieldType.ENTITY:
                continue
            raw_field_data: JsSchemaEntityFieldData | None = field._js_schema
            if raw_field_data and raw_field_data.inverse_association:
                inverse_links = []
                # string means it's a one to one link
                if isinstance(raw_field_data.inverse_association, str):
                    inverse_entity_name, inverse_field_name = raw_field_data.inverse_association.split(".")
                    inverse_entity = schema.get_entity(inverse_entity_name)
                    assert inverse_entity is not None
                    inverse_field = inverse_entity[inverse_field_name]
                    field.link = SingleEntityLink([inverse_field])
                    inverse_links.append(inverse_field)
                else:
                    # list means it's a one to many link
                    inverse_links = []
                    for inverse_association in raw_field_data.inverse_association:
                        # make a link for this association
                        inverse_entity_name, inverse_field_name = inverse_association.split(".")
                        inverse_entity = schema.get_entity(inverse_entity_name)
                        assert inverse_entity is not None
                        inverse_field = inverse_entity[inverse_field_name]
                        inverse_links.append(inverse_field)
                    field.link = SingleEntityLink(inverse_links)

                # Make links on the inverse fields
                for inverse_field in inverse_links:
                    inverse_field.link = ReverseOfSingleEntityLink(field)
