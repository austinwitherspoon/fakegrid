from dataclasses import dataclass, field
import datetime
import json
from os import PathLike
import os
import pickle
from typing import Dict, Any, List, Optional, Tuple, Union
from typing_extensions import Self
from enum import Enum
import re
import requests

import shotgun_api3


DEFAULT_REVERSES: Dict[str, Dict[str, Optional[str]]] = {
    "_": {
        "notes": "note_links",
        "tags": None,
        "open_notes": None,  # dynamic?
        "users": None,
        "user": None,
        "tasks": "entity",
        "addressings_to": None,
        "addressings_cc": None,
        "projects": None,
        "created_by": None,
        "updated_by": None,
        "image_source_entity": None,
        "project": None,
        "cuts": "entity",
        "template_task": None,
        "task_template": None,
        "attachments": "attachment_links",
    },
    "ActionMenuItem": {
        "permissions_groups": None,
    },
    "ApiUser": {
        "permission_rule_set": None,
    },
    "Asset": {
        "parents": "assets",
        "task_template": None,
    },
    "Attachment": {
        "attachment_reference_links": None,
        "local_storage": None,
    },
    "Composition": {
        "composition_links": "composition",
        "content": None,
    },
    "Cut": {
        "sg_scene": None,
        "attachments": None,
    },
    "CutItem": {
        "shot": None,
        "version": None,
    },
    "Delivery": {
        "attachments": None,
        "sg_from": None,
    },
    "Department": {
        "users": "department",
    },
    "EventLogEntry": {
        "user": None,
        "entity": None,
    },
    "FilesystemLocation": {
        "entity": None,
        "pipeline_configuration": None,
    },
    "Group": {
        "sg_people": "sg_vendor_group",
        "launch_sg_vendor_groups_launches": "sg_vendor_groups",
    },
    "HumanUser": {
        "projects": "users",
        "banners": "users",
        "bookings": "user",
        "contracts": "user",
        "custom_home_page": None,
        "groups": "users",
        "permission_rule_set": None,
        "pipeline_configurations": "users",
        "app_welcomes": "users",
    },
    "Level": {
        "sub_levels": "parent",
    },
    "Launch": {
        "scenes": "launches",
    },
    "MimEntity": {
        "mim_project": None,
    },
    "MimField": {
        "display_column": None,
        "mim_entity": None,
    },
    "MocapSetup": {
        "sg_shoot_day": None,
    },
    "MocapTake": {
        "sg_shoot_day": None,
        "sg_setup": None,
        "physical_assets": None,
        "sg_routine": None,
        "sg_scene": None,
    },
    "Note": {
        "notes_app_context": None,
        "composition": None,
    },
    "PageHit": {
        "page": None,
    },
    "PageSetting": {
        "page": None,
    },
    "Performer": {
        "sg_character": None,
    },
    "PermissionRuleSet": {
        "parent_set": None,
    },
    "Playlist": {
        "locked_by": None,
    },
    "PlaylistShare": {
        "user": None,
        "revoked_by": None,
        "playlist": None,
    },
    "Project": {
        "phases": "projects",
        "layout_project": None,
    },
    "PublishEvent": {
        "publish_event_links": None,
        "sg_link": None,
    },
    "PublishedFile": {
        "downstream_published_files": "upstream_published_files",
        "path_cache_storage": None,
        "task": None,
        "published_file_type": None,
    },
    "PublishedFileDependency": {
        "dependent_published_file": None,
        "published_file": None,
    },
    "Reply": {
        "user": None,
        "entity": "replies",
    },
    "Revision": {
        "published_files": "revision",
        "revisions": None,
        "sg_asset": None,
    },
    "Routine": {
        "sg_scene": None,
        "sg_shoot_day": None,
    },
    "RvLicense": {
        "site": None,
    },
    "Scene": {},
    "Sequence": {
        "sg_scenes": "sg_sequence",
    },
    "Shot": {
        "parent_shots": "shots",
        "sg_sequence": "shots",
        "sg_scene": "shots",
        "launches": "shots",
    },
    "Software": {
        "user_restrictions": None,
    },
    "SourceClip": {
        "source_clip_versions": "source_clip",
    },
    "Status": {
        "icon": None,
    },
    "Task": {
        "sg_versions": "tasks",
        "downstream_tasks": "upstream_tasks",
        "sibling_tasks": None,  # dynamic?
        "step": None,
        "task_assignees": None,
        "task_reviewers": None,
    },
    "TaskDependency": {
        "dependent_task": None,
        "task": None,
    },
    "Ticket": {
        "sg_ticket_type": "tickets",
        "tickets": None,
        "replies": None,
        "attachments": None,
    },
    "TimeLog": {
        "user": None,
        "entity": None,
    },
    "Version": {
        "user": None,
        "sg_task": None,
        "client_approved_by": None,
    },
}


DEFAULT_CONNECTIONS = {
    "ApiUser": {
        "projects": ("ApiUserProjectConnection", "user"),
    },
    "Asset": {
        "assets": ("AssetAssetConnection", "parent"),
        "parents": ("AssetAssetConnection", "asset"),
        "blendshapes": ("AssetBlendshapeConnection", "asset"),
        "elements": ("AssetElementConnection", "asset"),
        "episodes": ("AssetEpisodeConnection", "asset"),
        "levels": ("AssetLevelConnection", "asset"),
        "mocap_takes": ("AssetMocapTakeConnection", "asset"),
        "scenes": ("AssetSceneConnection", "asset"),
        "sequences": ("AssetSequenceConnection", "asset"),
        "shoot_days": ("AssetShootDayConnection", "asset"),
        "shots": ("AssetShotConnection", "asset"),
    },
    "Banner": {
        "users": ("BannerUserConnection", "banner"),
    },
    "BlendShape": {
        "assets": ("AssetBlendshapeConnection", "blendshape"),
    },
    "Camera": {
        "mocap_takes": ("CameraMocapTakeConnection", "camera"),
    },
    "Element": {
        "assets": ("AssetElementConnection", "element"),
        "shots": ("ElementShotConnection", "element"),
    },
    "Episode": {
        "assets": ("AssetEpisodeConnection", "episode"),
    },
    "Group": {
        "users": ("GroupUserConnection", "group"),
    },
    "HumanUser": {
        "banners": ("BannerUserConnection", "user"),
        "groups": ("GroupUserConnection", "user"),
        "pipeline_configurations": ("PipelineConfigurationUserConnection", "user"),
        "projects": ("ProjectUserConnection", "user"),
    },
    "Launch": {
        "scenes": ("LaunchSceneConnection", "launch"),
        "shots": ("LaunchShotConnection", "launch"),
    },
    "Level": {
        "assets": ("AssetLevelConnection", "level"),
    },
    "MocapTake": {
        "assets": ("AssetMocapTakeConnection", "mocap_take"),
        "cameras": ("CameraMocapTakeConnection", "mocap_take"),
        "performers": ("PerformerMocapTakeConnection", "mocap_take"),
        "physical_assets": ("PhysicalAssetMocapTakeConnection", "mocap_take"),
    },
    "MocapTakeRange": {
        "shots": ("MocapTakeRangeShotConnection", "mocap_take_range"),
    },
    "Performer": {
        "mocap_takes": ("PerformerMocapTakeConnection", "performer"),
        "routines": ("PerformerRoutineConnection", "performer"),
        "shoot_days": ("PerformerShootDayConnection", "performer"),
    },
    "PhysicalAsset": {
        "mocap_takes": ("PhysicalAssetMocapTakeConnection", "physical_asset"),
    },
    "PipelineConfiguration": {
        "users": ("PipelineConfigurationUserConnection", "pipeline_configuration"),
    },
    "Playlist": {
        "versions": ("PlaylistVersionConnection", "playlist"),
    },
    "Project": {
        "users": ("ProjectUserConnection", "project"),
        "task_templates": ("ProjectTaskTemplateConnection", "project"),
    },
    "Release": {
        "tickets": ("ReleaseTicketConnection", "release"),
    },
    "Revision": {
        "revisions": ("RevisionRevisionConnection", "dest_revision"),
        "tickets": ("RevisionTicketConnection", "revision"),
    },
    "Routine": {
        "performers": ("PerformerRoutineConnection", "routine"),
    },
    "Sequence": {
        "assets": ("AssetSequenceConnection", "sequence"),
    },
    "Scene": {
        "assets": ("AssetSceneConnection", "scene"),
        "launches": ("LaunchSceneConnection", "scene"),
        "shoot_days": ("ShootDaySceneConnection", "scene"),
    },
    "ShootDay": {
        "assets": ("AssetShootDayConnection", "shoot_day"),
        "performers": ("PerformerShootDayConnection", "shoot_day"),
        "scenes": ("ShootDaySceneConnection", "shoot_day"),
    },
    "Shot": {
        "assets": ("AssetShotConnection", "shot"),
        "elements": ("ElementShotConnection", "shot"),
        "launches": ("LaunchShotConnection", "shot"),
        "mocap_take_ranges": ("MocapTakeRangeShotConnection", "shot"),
        "parent_shots": ("ShotShotConnection", "shot"),
        "shots": ("ShotShotConnection", "parent_shot"),
    },
    "TaskTemplate": {
        "projects": ("ProjectTaskTemplateConnection", "task_template"),
    },
    "Ticket": {
        "releases": ("ReleaseTicketConnection", "ticket"),
        "tickets": ("TicketTicketConnection", "source_ticket"),
        "revisions": ("RevisionTicketConnection", "ticket"),
    },
    "Version": {
        "playlists": ("PlaylistVersionConnection", "version"),
    },
}


class FieldType(Enum):
    Text = "text"
    Float = "float"
    MultiEntity = "multi_entity"
    Number = "number"
    Addressing = "addressing"
    Checkbox = "checkbox"
    Color = "color"
    Currency = "currency"
    Date = "date"
    DateTime = "date_time"
    Duration = "duration"
    Entity = "entity"
    Footage = "footage"
    Image = "image"
    List = "list"
    Password = "password"
    Percent = "percent"
    Serializable = "serializable"
    StatusList = "status_list"
    Summary = "summary"
    TagList = "tag_list"
    Timecode = "timecode"
    Url = "url"
    EntityType = "entity_type"
    PivotColumn = "pivot_column"
    Uuid = "uuid"
    JsonB = "jsonb"
    Calculated = "calculated"


"""addressing                  'is'
                            'is_not'
                            'contains'
                            'not_contains'
                            'in'
                            'type_is'
                            'type_is_not'
                            'name_contains'
                            'name_not_contains'
                            'name_starts_with'
                            'name_ends_with'

checkbox                    'is'
                            'is_not'

currency                    'is'
                            'is_not'
                            'less_than'
                            'greater_than'
                            'between'
                            'not_between'
                            'in'
                            'not_in'

date                        'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'in_last'
                            'not_in_last'
                            'in_next'
                            'not_in_next'
                            'in_calendar_day'
                            'in_calendar_week'
                            'in_calendar_month'
                            'in_calendar_year'
                            'between'
                            'in'
                            'not_in'

date_time                   'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'in_last'
                            'not_in_last'
                            'in_next'
                            'not_in_next'
                            'in_calendar_day'
                            'in_calendar_week'
                            'in_calendar_month'
                            'in_calendar_year'
                            'between'
                            'in'
                            'not_in'

duration                    'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'between'
                            'in'
                            'not_in'

entity                      'is'
                            'is_not'
                            'type_is'
                            'type_is_not'
                            'name_contains'
                            'name_not_contains'
                            'name_is'
                            'in'
                            'not_in'

float                       'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'between'
                            'in'
                            'not_in'

image                       'is' ** Note: For both 'is' and 'is_not', the only supported value is None,
                            'is_not' **  which supports detecting the presence or lack of a thumbnail.

list                        'is'
                            'is_not'
                            'in'
                            'not_in'

multi_entity                'is' ** Note:  when used on multi_entity, this functions as
                                            you would expect 'contains' to function
                            'is_not'
                            'type_is'
                            'type_is_not'
                            'name_contains'
                            'name_not_contains'
                            'in'
                            'not_in'

number                      'is'
                            'is_not'
                            'less_than'
                            'greater_than'
                            'between'
                            'not_between'
                            'in'
                            'not_in'

password                    ** Filtering by this data type field not supported

percent                     'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'between'
                            'in'
                            'not_in'

serializable                ** Filtering by this data type field not supported

status_list                 'is'
                            'is_not'
                            'in'
                            'not_in'

summary                     ** Filtering by this data type field not supported


tag_list                    'is'  ** Note:  when used on tag_list, this functions as
                                            you would expect 'contains' to function
                            'is_not'
                            'name_contains'
                            'name_not_contains'
                            'name_id'

text                        'is'
                            'is_not'
                            'contains'
                            'not_contains'
                            'starts_with'
                            'ends_with'
                            'in'
                            'not_in'


timecode                    'is'
                            'is_not'
                            'greater_than'
                            'less_than'
                            'between'
                            'in'
                            'not_in'"""

ALLOWED_OPERATIONS_BY_FIELD_TYPE = {
    FieldType.Text: [
        "is",
        "is_not",
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
        "in",
        "not_in",
    ],
    FieldType.Float: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.MultiEntity: [
        "is",
        "is_not",
        "type_is",
        "type_is_not",
        "name_contains",
        "name_not_contains",
        "name_is",
        "in",
        "not_in",
    ],
    FieldType.Number: [
        "is",
        "is_not",
        "less_than",
        "greater_than",
        "between",
        "not_between",
        "in",
        "not_in",
    ],
    FieldType.Addressing: [
        "is",
        "is_not",
        "contains",
        "not_contains",
        "in",
        "type_is",
        "type_is_not",
        "name_contains",
        "name_not_contains",
        "name_starts_with",
        "name_ends_with",
    ],
    FieldType.Checkbox: ["is", "is_not"],
    FieldType.Color: ["is", "is_not", "in", "not_in"],
    FieldType.Currency: [
        "is",
        "is_not",
        "less_than",
        "greater_than",
        "between",
        "not_between",
        "in",
        "not_in",
    ],
    FieldType.Date: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "in_last",
        "not_in_last",
        "in_next",
        "not_in_next",
        "in_calendar_day",
        "in_calendar_week",
        "in_calendar_month",
        "in_calendar_year",
        "between",
        "in",
        "not_in",
    ],
    FieldType.DateTime: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "in_last",
        "not_in_last",
        "in_next",
        "not_in_next",
        "in_calendar_day",
        "in_calendar_week",
        "in_calendar_month",
        "in_calendar_year",
        "between",
        "in",
        "not_in",
    ],
    FieldType.Duration: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.Entity: [
        "is",
        "is_not",
        "type_is",
        "type_is_not",
        "name_contains",
        "name_not_contains",
        "name_is",
        "in",
        "not_in",
    ],
    FieldType.Footage: ["is", "is_not"],
    FieldType.Image: ["is", "is_not"],
    FieldType.List: ["is", "is_not", "in", "not_in"],
    FieldType.Password: [],
    FieldType.Percent: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.Serializable: [],
    FieldType.StatusList: ["is", "is_not", "in", "not_in"],
    FieldType.Summary: [],
    FieldType.TagList: [
        "is",
        "is_not",
        "name_contains",
        "name_not_contains",
        "name_id",
    ],
    FieldType.Timecode: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.Url: [],
}


@dataclass
class ShotgridField:
    entity: "ShotgridEntity" = field(repr=False)
    api_name: str
    display_name: str
    field_type: FieldType
    properties: Dict[str, Any] = field(repr=False)
    parent_of: Optional[List["ShotgridField"]] = field(default=None, repr=False)
    reverse_of: Optional["ShotgridField"] = field(default=None, repr=False)
    connection_entity: Optional[Tuple["ShotgridEntity", str]] = field(
        default=None, repr=False
    )
    one_sided: bool = field(default=False, repr=False)

    @property
    def connection_query_target_field(self) -> Optional["ShotgridField"]:
        """Get the field we're querying if accessing via connection table"""
        if self.field_type not in [
            FieldType.MultiEntity,
            FieldType.Entity,
            FieldType.Addressing,
            FieldType.TagList,
        ]:
            return None

        if not self.entity.api_name.endswith("Connection"):
            # this happens when we're the reverse of a single entity link field,
            # So there's no connection table, but we want to return the parent field
            return None if self.field_type != FieldType.MultiEntity else self.reverse_of

        possible_targets = [
            f
            for f in self.entity.fields.values()
            if f.field_type == FieldType.Entity
            and f != self
            and not f.api_name.startswith("sg_")
        ]
        return next(iter(possible_targets), None)

    def from_database(self, value: Any) -> Any:
        if value is None:
            return None
        if self.field_type in [
            FieldType.Text,
            FieldType.Float,
            FieldType.Number,
            FieldType.Checkbox,
            FieldType.Currency,
            FieldType.Date,
            FieldType.DateTime,
            FieldType.Duration,
            FieldType.Footage,
            FieldType.Password,
            FieldType.Percent,
            FieldType.StatusList,
            FieldType.Summary,
            FieldType.Timecode,
            FieldType.EntityType,
            FieldType.PivotColumn,
            FieldType.Uuid,
            FieldType.JsonB,
        ]:
            return value
        if self.field_type in [
            FieldType.JsonB,
            FieldType.Serializable,
            FieldType.Url,
            FieldType.Entity,
            FieldType.MultiEntity,
        ]:
            return json.loads(value)
        return value

    def to_database(
        self,
        value: Any,
    ) -> Any:
        if value is None:
            return None
        if isinstance(value, list):
            return [self.to_database(v) for v in value]
        if self.field_type == FieldType.Text:
            return str(value)
        if self.field_type in [
            FieldType.Float,
            FieldType.Number,
            FieldType.Checkbox,
            FieldType.Currency,
            FieldType.Duration,
            FieldType.Footage,
            FieldType.Password,
            FieldType.Percent,
            FieldType.StatusList,
            FieldType.Summary,
            FieldType.Timecode,
            FieldType.EntityType,
            FieldType.PivotColumn,
            FieldType.Uuid,
            FieldType.JsonB,
        ]:
            return value
        if self.field_type == FieldType.Date:
            if isinstance(value, datetime.datetime):
                return value
            if value in ["MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR"]:
                return value
            if isinstance(value, int):
                return value
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        if self.field_type == FieldType.DateTime:
            # convert to utc
            return value

        if self.field_type in [FieldType.JsonB, FieldType.Serializable, FieldType.Url]:
            return json.dumps(value)
        return value


@dataclass
class ShotgridEntity:
    schema: "ShotgridSchema" = field(repr=False)
    api_name: str
    display_name: str
    fields: Dict[str, ShotgridField]
    real: bool = field(default=True)

    @property
    def name_field(self) -> Optional[str]:
        valid_name_fields = ["code", "name", "title", "content"]
        return next(
            (
                name_field
                for name_field in valid_name_fields
                if name_field in self.fields
            ),
            None,
        )


@dataclass
class ShotgridSchema:
    entities: Dict[str, ShotgridEntity]

    def save(self, file: Union[str, bytes, os.PathLike]) -> None:
        with open(file, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def from_file(cls, file: Union[str, bytes, os.PathLike]) -> Self:
        with open(file, "rb") as f:
            return pickle.load(f)

    @classmethod
    def from_connection(cls, sg: shotgun_api3.Shotgun) -> Self:
        entities = sg.schema_entity_read()
        display_names: Dict[str, str] = {
            k: v["name"]["value"] for k, v in entities.items()  # type: ignore
        }
        fields: Dict[str, Dict[str, Dict[str, Any]]] = sg.schema_read()  # type: ignore

        schema = cls(entities={})
        for entity_name, raw_fields in fields.items():
            shotgrid_fields: Dict[str, ShotgridField] = {}
            schema.entities[entity_name] = entity = ShotgridEntity(
                schema=schema,
                api_name=entity_name,
                display_name=display_names[entity_name],
                fields=shotgrid_fields,
            )
            for field_name, field_properties in raw_fields.items():
                try:
                    shotgrid_fields[field_name] = ShotgridField(
                        entity=entity,
                        api_name=field_name,
                        display_name=field_properties["name"]["value"],
                        field_type=FieldType(field_properties["data_type"]["value"]),
                        properties=field_properties,
                    )
                except ValueError:
                    print(
                        "Warning: Unknown field type: {}".format(
                            field_properties["data_type"]["value"]
                        )
                    )
                    continue

        # get the schema that the shotgrid website uses,
        # hopefully we can one day access this directly from the python api,
        # but right now this is the only place to get reverse fields
        js_schema = requests.get(
            f"{sg.base_url}/page/reload_schema",
            cookies={"_session_id": sg.get_session_token()},
        ).json()

        cls._resolve_reverses(schema, js_schema)
        cls._resolve_connections(schema)

        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type not in [FieldType.MultiEntity, FieldType.Entity]:
                    continue
                if (
                    (not _field.reverse_of)
                    and (not _field.parent_of)
                    and (not _field.one_sided)
                ):
                    print(
                        f"Warning: {entity.api_name}.{_field.api_name} ({_field.display_name}) "
                        "has no reverse field."
                    )
                if (
                    _field.field_type != FieldType.Entity
                    and _field.reverse_of
                    and _field.reverse_of.field_type != FieldType.Entity
                    and (not _field.one_sided)
                    and (not _field.connection_entity)
                ):
                    print(
                        f"Warning: {entity.api_name}.{_field.api_name} ({_field.display_name}) "
                        "has no connection entity."
                    )

        return schema

    @classmethod
    def _resolve_connections(cls, schema: Self) -> None:
        for entity in list(schema.entities.values()):
            if entity.api_name.endswith("Connection"):
                continue
            for field in entity.fields.values():  # noqa
                if field.field_type != FieldType.MultiEntity:
                    continue
                if field.connection_entity:
                    continue
                if field.reverse_of and field.reverse_of.field_type == FieldType.Entity:
                    continue
                real_connection_entity = next(
                    (
                        e
                        for e in schema.entities.values()
                        if e.api_name.endswith("Connection")
                        and any(f.reverse_of == field for f in e.fields.values())
                    ),
                    None,
                )
                if real_connection_entity:
                    connection_field = next(
                        (
                            f
                            for f in real_connection_entity.fields.values()
                            if f.reverse_of == field
                        ),
                    )
                    field.connection_entity = (
                        real_connection_entity,
                        connection_field.api_name,
                    )
                    continue
                else:
                    # Lets make a fake connection entity!

                    connection_entity_name = (
                        f"fakegrid_{entity.api_name}_{field.api_name}_Connection"
                    )
                    connection_entity = ShotgridEntity(
                        schema=schema,
                        api_name=connection_entity_name,
                        display_name=connection_entity_name,
                        fields={},
                        real=False,
                    )
                    # id field
                    connection_entity.fields["id"] = ShotgridField(
                        entity=connection_entity,
                        api_name="id",
                        display_name="id",
                        field_type=FieldType.Number,
                        properties={
                            "name": {"value": "id"},
                            "data_type": {"value": "number"},
                            "properties": {},
                        },
                    )
                    # parent field
                    parent_api_name = snake_case(entity.api_name)
                    if entity.api_name in field.properties["properties"]["valid_types"]:
                        parent_api_name = f"source_{parent_api_name}"

                    connection_entity.fields[parent_api_name] = ShotgridField(
                        entity=connection_entity,
                        api_name=parent_api_name,
                        display_name=entity.display_name,
                        field_type=FieldType.Entity,
                        properties={
                            "name": {"value": entity.display_name},
                            "data_type": {"value": "entity"},
                            "properties": {
                                "valid_types": {"value": [entity.api_name]},
                                "connection": {"value": "parent"},
                            },
                        },
                        one_sided=True,
                    )
                    field.connection_entity = (
                        connection_entity,
                        parent_api_name,
                    )

                    # make a single child field for all valid types
                    entity_api_name = "linked_entity"
                    child_field_key_field = ShotgridField(
                        entity=connection_entity,
                        api_name=entity_api_name,
                        display_name=entity_api_name,
                        field_type=FieldType.Entity,
                        properties={
                            "name": {"value": entity_api_name},
                            "data_type": {"value": "entity"},
                            "properties": {
                                "valid_types": {
                                    "value": field.properties["properties"][
                                        "valid_types"
                                    ]["value"]
                                },
                                "connection": {"value": "child"},
                            },
                        },
                        one_sided=True,
                    )
                    connection_entity.fields[entity_api_name] = child_field_key_field

                    for child_field in field.parent_of or []:
                        child_field.connection_entity = (
                            connection_entity,
                            entity_api_name,
                        )

                    schema.entities[connection_entity_name] = connection_entity

    @classmethod
    def _resolve_reverses(cls, schema: Self, js_schema: dict) -> None:
        """Identifies reverse fields and updates metadata to reflect these."""

        cls._resolve_builtin_reverses(schema)

        for entity, fields in js_schema["schema"]["entity_fields"].items():
            if entity not in schema.entities:
                continue
            for field_name, field_properties in fields.items():
                fakegrid_field = schema.entities[entity].fields.get(field_name)
                if not fakegrid_field:
                    continue
                reverse_of = field_properties.get("reverse_of")
                inverse_association = field_properties.get("inverse_association")
                if entity == "Shot" and field_name == "assets":
                    print(entity, field_name, reverse_of, inverse_association)

                if not reverse_of and not inverse_association:
                    continue
                if reverse_of:
                    reverse_entity_name = reverse_of["entity_type_name"]
                    reverse_field_name = reverse_of["name"]
                    if reverse_entity_name not in schema.entities:
                        continue
                    if (
                        reverse_field_name
                        not in schema.entities[reverse_entity_name].fields
                    ):
                        continue
                    reverse_field = schema.entities[reverse_entity_name].fields[
                        reverse_field_name
                    ]

                else:
                    if isinstance(inverse_association, list):
                        continue
                    reverse_entity_name, reverse_field_name = inverse_association.split(
                        "."
                    )
                    connection_entity_name = field_properties.get(
                        "through_join_entity_type"
                    )
                    if (
                        connection_entity_name
                        and connection_entity_name in schema.entities
                    ):
                        # we have a connection entity defined, check if this field is the "child" field
                        if connection_entity_name.startswith(entity):
                            continue

                    if reverse_entity_name not in schema.entities:
                        continue
                    if (
                        reverse_field_name
                        not in schema.entities[reverse_entity_name].fields
                    ):
                        continue
                    reverse_field = schema.entities[reverse_entity_name].fields[
                        reverse_field_name
                    ]

                fakegrid_field.reverse_of = reverse_field
                if reverse_field.parent_of:
                    reverse_field.parent_of.append(fakegrid_field)
                else:
                    reverse_field.parent_of = [fakegrid_field]

    @classmethod
    def _resolve_builtin_reverses(cls, schema: Self) -> None:
        built_in_entities = r"^({entities})$".format(
            entities="|".join([snake_case(e) for e in schema.entities])
        )
        built_in_multientities = r"^({entities})s$".format(
            entities="|".join([snake_case(e) for e in schema.entities])
        )

        # first lets get defaults
        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type not in [
                    FieldType.MultiEntity,
                    FieldType.Entity,
                    FieldType.TagList,
                    FieldType.Addressing,
                ]:
                    continue
                if _field.reverse_of or _field.parent_of or _field.one_sided:
                    continue

                try:
                    matching_default = DEFAULT_REVERSES.get(
                        _field.entity.api_name, DEFAULT_REVERSES["_"]
                    )[_field.api_name]
                except KeyError:
                    try:
                        matching_default = DEFAULT_REVERSES["_"][_field.api_name]
                    except KeyError:
                        continue

                if not matching_default:
                    _field.one_sided = True
                    continue

                parent_field_name = matching_default
                allowed_types = _field.properties["properties"]["valid_types"]["value"]
                if not allowed_types:
                    _field.one_sided = True
                    continue
                for _type in allowed_types:
                    parent_entity = schema.entities.get(_type)
                    if not parent_entity:
                        continue
                    parent_field = parent_entity.fields.get(parent_field_name)
                    if not parent_field:
                        continue

                    _field.reverse_of = parent_field
                    if isinstance(parent_field.parent_of, list):
                        parent_field.parent_of.append(_field)
                    else:
                        parent_field.parent_of = [_field]

        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type != FieldType.Entity:
                    continue
                if _field.reverse_of or _field.parent_of or _field.one_sided:
                    continue
                match = re.match(built_in_entities, _field.api_name)
                if not match:
                    continue

                child_entity_type = camel_case(match[1])
                if child_entity_type == entity.api_name:
                    continue
                child_field_name = f"{snake_case(entity.api_name)}s"
                child_entity = schema.entities.get(child_entity_type)
                if not child_entity:
                    continue
                child_field = child_entity.fields.get(child_field_name)
                if (
                    not child_field
                    or not child_field.field_type == FieldType.MultiEntity
                ):
                    continue
                _field.parent_of = [child_field]
                child_field.reverse_of = _field

        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type != FieldType.MultiEntity:
                    continue
                if _field.reverse_of or _field.parent_of or _field.one_sided:
                    continue
                match = re.match(built_in_multientities, _field.api_name)
                if not match:
                    continue

                child_entity_type = camel_case(match[1])
                if child_entity_type == entity.api_name:
                    continue
                child_field_name = snake_case(entity.api_name) + "s"
                child_entity = schema.entities.get(child_entity_type)
                if not child_entity:
                    continue
                child_field = child_entity.fields.get(child_field_name)
                if not child_field:
                    continue
                _field.reverse_of = child_field
                if isinstance(child_field.parent_of, list):
                    child_field.parent_of.append(_field)
                else:
                    child_field.parent_of = [_field]


def snake_case(name):
    return "".join(["_" + i.lower() if i.isupper() else i for i in name]).lstrip("_")


def camel_case(name):
    return "".join([i.capitalize() for i in name.split("_")])
