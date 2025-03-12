"""Parse the Shotgun JS API schema data and build our fakegrid schema.


What we know:
- `reverse_fields` is filled in on a single entity link, showing all reverse fields.
- multi-entity links USUALLY have a through_join_field and through_join_entity_type.
- SOMETIMES inverse_association or reverse_of is filled in on either a multi or single entity link
- `primary_side_of` is sometimes filled in on multi entity links, similar to `inverse_association` or `reverse_of`

"""

from fakegrid.schema import Entity, Field, FieldType, ManyToManyLink, OneToManyLink, OneToOneLink, Schema

from .js_schema import JsSchema, JsSchemaEntityFieldData

IGNORE_CONNECTION_ENTITIES = {
    "NoteLink",  # missing reverse fields
    "Tagging",  # missing reverse fields
    "NoteTask",  # missing reverse fields
    "Attachment",  # doesn't exist
    "AttachmentLink",  # doesn't exist
    "ActionMenuItemProjectConnection",  # doesn't exist
    "ActionMenuItemPermissionConnection",  # doesn't exist
    "ProjectSoftwareConnection",  # doesn't exist
    "TankActionPermissionRuleSetDenial",
}


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
        schema.entities.append(entity)

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
            entity.fields.append(field)

    resolve_multi_entity_links(schema)
    resolve_single_entity_links(schema)

    return schema


def resolve_multi_entity_links(schema: Schema) -> None:
    """Resolve multi-entity links."""
    for entity in schema.entities:
        for field in entity.fields:
            if field.field_type != FieldType.MULTI_ENTITY:
                continue
            raw_field_data: JsSchemaEntityFieldData = field._js_schema  # type: ignore
            if raw_field_data.through_join_field and raw_field_data.through_join_entity_type:
                if raw_field_data.through_join_entity_type in IGNORE_CONNECTION_ENTITIES:
                    continue
                connection_entity = schema.get_entity(raw_field_data.through_join_entity_type)
                assert connection_entity is not None
                connection_field = next(
                    (
                        f
                        for f in connection_entity.fields
                        if f._js_schema.inverse_association == f"{entity.api_name}.{field.api_name}"  # type: ignore
                    ),
                    None,
                )
                assert connection_field is not None
                field.link = ManyToManyLink(connection_entity, (field, connection_field))


def resolve_single_entity_links(schema: Schema) -> None:
    """Resolve single-entity links."""
    for entity in schema.entities:
        for field in entity.fields:
            if field.field_type != FieldType.ENTITY:
                continue
            raw_field_data: JsSchemaEntityFieldData = field._js_schema  # type: ignore
            if raw_field_data.inverse_association:
                # string means it's a one to one link
                if isinstance(raw_field_data.inverse_association, str):
                    inverse_entity_name, inverse_field_name = raw_field_data.inverse_association.split(".")
                    inverse_entity = schema.get_entity(inverse_entity_name)
                    assert inverse_entity is not None
                    inverse_field = inverse_entity[inverse_field_name]
                    field.link = OneToOneLink(field, inverse_field)
                else:
                    # list means it's a one to many link
                    inverse_links = []
                    for inverse_association in raw_field_data.inverse_association:
                        inverse_entity_name, inverse_field_name = inverse_association.split(".")
                        inverse_entity = schema.get_entity(inverse_entity_name)
                        assert inverse_entity is not None
                        inverse_field = inverse_entity[inverse_field_name]
                        inverse_links.append(inverse_field)
                    field.link = OneToManyLink(field, inverse_links)
