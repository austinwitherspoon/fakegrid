"""Models that represent the schema data returned by the javascript shotgun API."""

from dataclasses import dataclass


@dataclass
class JsSchema:
    """The shotgun schema data that is returned by the javascript shotgun API."""

    entity_fields: dict[str, dict[str, "JsSchemaEntityFieldData"]]

    @classmethod
    def from_dict(cls, data: dict) -> "JsSchema":
        return cls(
            entity_fields={
                entity_type: {
                    field_name: JsSchemaEntityFieldData.from_dict(field_data)
                    for field_name, field_data in fields.items()
                }
                for entity_type, fields in data["schema"]["entity_fields"].items()
            }
        )


@dataclass
class JsSchemaEntityFieldData:
    """A single field in shotgun's javascript schema data.

    We use this instead of the python API because it contains more information about linked entities.

    What we know:
     - reverse_fields is filled in on a single entity link, showing all reverse fields.
     - multi-entity links USUALLY have a through_join_field and through_join_entity_type.
     - SOMETIMES inverse_association or reverse_of is filled in on either a multi or single entity link
    """

    id: int
    entity_type: str
    name: str
    display_name: str
    description: str | None
    data_type: str
    summary_default: str
    grid_column: bool
    configurable: bool
    field_type: str
    custom_metadata: dict | None
    image_use_query: bool
    editor_properties: dict
    is_aggregate: bool
    is_derived: bool
    allowed_entity_types: list[str] | None = None
    through_join_field: str | None = None
    through_join_entity_type: str | None = None
    entity_types_nonconfigurable: bool | None = None
    editor: str | None = None
    inverse_association: str | list[str] | None = None
    primary_side_of: "PrimarySideOf | None" = None
    reverse_of: "ReverseOf | None" = None
    has_reverse_fields: bool | None = None
    reverse_fields: dict[str, str] | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "JsSchemaEntityFieldData":
        return cls(
            id=data["id"],
            entity_type=data["entity_type"],
            name=data["name"],
            display_name=data["display_name"],
            description=data["description"],
            data_type=data["data_type"],
            summary_default=data["summary_default"],
            grid_column=data["grid_column"],
            configurable=data["configurable"],
            field_type=data["field_type"],
            custom_metadata=data["custom_metadata"],
            image_use_query=data["image_use_query"],
            editor_properties=data["editor_properties"],
            is_aggregate=data["is_aggregate"],
            is_derived=data["is_derived"],
            allowed_entity_types=data.get("allowed_entity_types"),
            through_join_field=data.get("through_join_field"),
            through_join_entity_type=data.get("through_join_entity_type"),
            entity_types_nonconfigurable=data.get("entity_types_nonconfigurable"),
            editor=data.get("editor"),
            inverse_association=data.get("inverse_association"),
            primary_side_of=PrimarySideOf.from_dict(data["primary_side_of"]) if data.get("primary_side_of") else None,
            reverse_of=ReverseOf.from_dict(data["reverse_of"]) if data.get("reverse_of") else None,
            has_reverse_fields=data.get("has_reverse_fields"),
            reverse_fields=data.get("reverse_fields"),
        )


@dataclass
class PrimarySideOf:
    entity_type: str
    field_name: str

    @classmethod
    def from_dict(cls, data: dict) -> "PrimarySideOf":
        return cls(
            entity_type=data["entity_type"],
            field_name=data["field_name"],
        )


@dataclass
class ReverseOf:
    name: str
    entity_type_name: str

    @classmethod
    def from_dict(cls, data: dict) -> "ReverseOf":
        return cls(
            name=data["name"],
            entity_type_name=data["entity_type_name"],
        )
