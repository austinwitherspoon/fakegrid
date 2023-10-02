from os import name
from typing import Any, Dict, Iterable, List, Optional, Type, Union
from .schema import ShotgridEntity, ShotgridSchema, camel_case, snake_case, FieldType
from sqlalchemy.orm import (
    mapped_column,
    DeclarativeBase,
    Session,
    aliased,
    column_property,
    relationship,
)
from sqlalchemy import (
    Cast,
    ClauseElement,
    Column,
    ColumnElement,
    Date,
    Index,
    Join,
    MetaData,
    column,
    create_engine,
    Enum,
    String,
    Integer,
    JSON,
    Boolean,
    Float,
    Text,
    distinct,
    func,
    literal,
    or_,
    not_,
    and_,
    cast,
    DateTime,
    StaticPool,
    insert,
    update,
)

INDEX_FIELDS = ["code", "id", "name", "type", "sg_status_list"]

FIELD_TYPES_TO_SQL_ALCHEMY_TYPES = {
    FieldType.Text: Text,
    FieldType.Float: Float,
    FieldType.MultiEntity: None,
    FieldType.Number: Integer,
    FieldType.Addressing: None,
    FieldType.Checkbox: Boolean,
    FieldType.Color: String,
    FieldType.Currency: Float,
    FieldType.Date: Date,
    FieldType.DateTime: DateTime,
    FieldType.Duration: Integer,
    FieldType.Entity: None,
    FieldType.Footage: String,
    FieldType.Image: String,
    FieldType.List: String,
    FieldType.Password: String,
    FieldType.Percent: Integer,
    FieldType.Serializable: Text,
    FieldType.StatusList: String,
    FieldType.Summary: Text,
    FieldType.TagList: None,
    FieldType.Timecode: Integer,
    FieldType.Url: Text,
    FieldType.EntityType: String,
    FieldType.PivotColumn: None,
    FieldType.Uuid: String,
    FieldType.JsonB: None,
    FieldType.Calculated: None,
}

IGNORED_TYPES = [
    FieldType.PivotColumn,
    FieldType.Calculated,
    FieldType.Summary,
    FieldType.JsonB,
]


class Base(DeclarativeBase):
    _entity: ShotgridEntity

    def __repr__(self):
        return f"<{self._entity.api_name} {self.to_dict()}>"

    def to_dict(
        self,
        fields: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        data = {"type": self._entity.api_name, "id": self.id}  # type:ignore
        fields = set(fields or list(self._entity.fields.keys()))
        for field in fields:
            if field in ["type", "id"]:
                continue

            _field_schema = self._entity.fields[field]
            if _field_schema.field_type in IGNORED_TYPES:
                data[field] = None
                continue

            elif _field_schema.field_type == FieldType.MultiEntity:
                continue

            _raw_value = getattr(self, field)
            data[field] = _field_schema.from_database(_raw_value)
        return data

    @classmethod
    def from_dict(cls, data):
        kwargs = {}
        for field_name, raw_value in data.items():
            if field_name in ["type"]:
                continue
            field_schema = cls._entity.fields[field_name]
            if field_schema.field_type in IGNORED_TYPES:
                continue
            if field_schema.field_type == FieldType.Entity:
                if isinstance(raw_value, dict):
                    kwargs[f"{field_name}_id"] = raw_value["id"]
                    kwargs[f"{field_name}_type"] = raw_value["type"]
                    kwargs[f"{field_name}_name"] = raw_value.get(
                        cls._entity.name_field
                    ) or raw_value.get("name")
                else:
                    kwargs[f"{field_name}_id"] = None
                    kwargs[f"{field_name}_type"] = None
                    kwargs[f"{field_name}_name"] = None

                continue
            elif field_schema.field_type == FieldType.MultiEntity:
                continue
            kwargs[field_name] = field_schema.to_database(raw_value)
        return cls(**kwargs)

    def update_from_dict(self, data: dict):
        for field_name, raw_value in data.items():
            if field_name in ["type"]:
                continue
            field_schema = self._entity.fields[field_name]
            if field_schema.field_type in IGNORED_TYPES:
                continue
            if field_schema.field_type == FieldType.Entity:
                if isinstance(raw_value, dict):
                    setattr(self, f"{field_name}_id", raw_value["id"])
                    setattr(self, f"{field_name}_type", raw_value["type"])
                    setattr(
                        self,
                        f"{field_name}_name",
                        raw_value.get(self._entity.name_field) or raw_value.get("name"),
                    )
                else:
                    setattr(self, f"{field_name}_id", None)
                    setattr(self, f"{field_name}_type", None)
                    setattr(self, f"{field_name}_name", None)

                continue
            elif field_schema.field_type == FieldType.MultiEntity:
                continue
            setattr(self, field_name, field_schema.to_database(raw_value))


def schema_to_models(schema: ShotgridSchema) -> Dict[str, Type[Base]]:
    """dynamically create SqlAlchemy models from a ShotgridSchema"""
    models = {}

    for entity in schema.entities.values():
        if "id" not in entity.fields:
            continue
        attributes: Dict[str, Any] = {
            "__tablename__": snake_case(entity.api_name),
            "is_retired": mapped_column(Boolean(), default=False, index=True),
            "_entity": entity,
        }

        for field in sorted(
            entity.fields.values(), key=lambda x: (x.api_name == "id", x.api_name)
        ):
            if field.field_type in IGNORED_TYPES:
                continue
            sql_type = FIELD_TYPES_TO_SQL_ALCHEMY_TYPES.get(field.field_type)
            if sql_type is None:
                continue
            # required = field.properties.get("mandatory", {}).get("value", False)
            required = False
            kwargs = {}

            indexed = field.api_name in INDEX_FIELDS

            if field.api_name == "id":
                required = True
                indexed = True
                kwargs["primary_key"] = True

            attributes[
                field.api_name if field.api_name != "metadata" else "_metadata"
            ] = mapped_column(
                sql_type(),
                name=field.api_name,
                nullable=not required,
                index=indexed,
                **kwargs,
            )

        for field in entity.fields.values():
            if field.field_type == FieldType.Entity:
                id_field_name = f"{field.api_name}_id"
                attributes[id_field_name] = mapped_column(
                    Integer(), name=id_field_name, nullable=True, index=True
                )
                type_field_name = f"{field.api_name}_type"
                attributes[type_field_name] = mapped_column(
                    String(),
                    name=type_field_name,
                    nullable=True,
                    index=True,
                )
                name_field_name = f"{field.api_name}_name"
                attributes[name_field_name] = mapped_column(
                    String(),
                    name=name_field_name,
                    nullable=True,
                    index=False,
                )
                attributes[field.api_name] = column_property(
                    entity_sql_representation(
                        attributes[type_field_name],
                        cast(attributes[id_field_name], String),
                        func.coalesce(attributes[name_field_name], ""),
                    )
                )
            elif field.field_type == FieldType.MultiEntity:
                id_column = attributes["id"]
                if field.connection_entity is not None:
                    # if we have a connection entity, use that to join
                    conn_entity, conn_field = field.connection_entity

                    attributes[field.api_name] = relationship(
                        conn_entity.api_name,
                        foreign_keys=[id_column],
                        primaryjoin=f"{conn_entity.api_name}.{conn_field}_id == {entity.api_name}.id",
                        viewonly=True,
                    )
                else:
                    # otherwise, we need to look up from the reverse field
                    if field.reverse_of:
                        reverse_field = field.reverse_of
                        reverse_entity = reverse_field.entity
                        attributes[field.api_name] = relationship(
                            reverse_entity.api_name,
                            foreign_keys=[id_column],
                            primaryjoin=f"{reverse_entity.api_name}.{reverse_field.api_name}_id == {entity.api_name}.id",
                            viewonly=True,
                        )
                    else:
                        raise NotImplementedError(
                            f"Could not find connection entity for {field.api_name}"
                        )

        cls = type(entity.api_name, (Base,), attributes)
        models[entity.api_name] = cls

    return models


def entity_sql_representation(
    entity_type,
    entity_id,
    entity_name,
) -> ColumnElement:
    """Returns an SQLAlchemy expression that represents an entity link as a json string."""
    return (
        literal('{"id":')
        + (entity_id if isinstance(entity_id, int) else cast(entity_id, String))
        + ', "name":"'
        + (
            entity_name
            if isinstance(entity_name, str)
            else func.coalesce(entity_name, "")
        )
        + literal('", "type": "')
        + (entity_type if isinstance(entity_type, str) else cast(entity_type, String))
        + '"}'
    )


def multi_entity_sql_representation(
    entity_type,
    entity_id,
    entity_name,
) -> ColumnElement:
    return func.coalesce(
        literal("[")
        + func.group_concat(
            distinct(
                entity_sql_representation(
                    entity_type,
                    entity_id,
                    entity_name,
                )
            ),
        )
        + "]",
        "[]",
    )
