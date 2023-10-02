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

        cls._resolve_reverses(schema, sg)

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
        # first, lets get defaults
        for entity, connections in DEFAULT_CONNECTIONS.items():
            if entity not in schema.entities:
                continue
            for _field, (
                connection_entity_name,
                connection_field_name,
            ) in connections.items():
                if (
                    connection_entity_name not in schema.entities
                    or _field not in schema.entities[entity].fields
                ):
                    continue
                try:
                    connection_entity = schema.entities[connection_entity_name]
                    connection_field = connection_entity.fields[connection_field_name]
                    connection_field.one_sided = True

                    schema.entities[entity].fields[_field].connection_entity = (
                        connection_entity,
                        connection_field_name,
                    )

                except KeyError as e:
                    print(e)
                    print(
                        f"Warning: Connection {entity}.{_field} -> "
                        f"{connection_entity_name}.{connection_field_name} "
                        "could not be resolved, skipping."
                    )
                    continue

        # now lets resolve existing connection tables
        for _, entity in schema.entities.items():
            if not entity.api_name.endswith("Connection"):
                continue

            dynamic_name = re.search(r"(\w+)_(sg_\w+)_Connection", entity.api_name)
            if not dynamic_name:
                continue

            parent_entity_type = dynamic_name[1]
            parent_field_name = dynamic_name[2]
            parent_key = snake_case(parent_entity_type)
            is_source_dest = False
            if parent_key not in entity.fields:
                if f"source_{parent_key}" in entity.fields:
                    parent_key = f"source_{parent_key}"
                    is_source_dest = True

            try:
                parent_entity = schema.entities[parent_entity_type]
                parent_field = parent_entity.fields[parent_field_name]
                parent_key_field = entity.fields[parent_key]
                parent_key_field.one_sided = True
                parent_field.connection_entity = (entity, parent_key)
                reverse_fields = parent_field.parent_of
                if not reverse_fields:
                    continue
                for reverse_field in reverse_fields:
                    reverse_key = snake_case(reverse_field.entity.api_name)
                    if is_source_dest:
                        reverse_key = f"dest_{reverse_key}"
                    reverse_key_field = entity.fields[reverse_key]
                    reverse_key_field.one_sided = True
                    reverse_field.connection_entity = (entity, reverse_key)
            except KeyError:
                print(
                    f"Warning: Connection {entity.api_name} could not "
                    "be resolved, skipping."
                )
                continue

        # now lets make fake connection entities for all other multi-entity links
        for _, entity in list(schema.entities.items()):
            if entity.api_name.endswith("Connection"):
                continue
            for _, _field in entity.fields.items():
                if _field.field_type != FieldType.MultiEntity:
                    continue
                if _field.connection_entity:
                    continue
                if _field.reverse_of:
                    continue

                connection_entity_name = (
                    f"fakegrid_{entity.api_name}_{_field.api_name}_Connection"
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
                is_source_dest = False
                if entity.api_name in _field.properties["properties"]["valid_types"]:
                    is_source_dest = True
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
                _field.connection_entity = (
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
                                "value": _field.properties["properties"]["valid_types"][
                                    "value"
                                ]
                            },
                            "connection": {"value": "child"},
                        },
                    },
                    one_sided=True,
                )
                connection_entity.fields[entity_api_name] = child_field_key_field

                for child_field in _field.parent_of or []:
                    child_field.connection_entity = (connection_entity, entity_api_name)

                schema.entities[connection_entity_name] = connection_entity

    @classmethod
    def _resolve_reverses(cls, schema: Self, sg: shotgun_api3.Shotgun) -> None:
        """Identifies reverse fields and updates metadata to reflect these."""

        # naming convention of multi_entity reverse fields, auto-generated by shotgun
        # e.g. "Version.sg_tasks" -> "Task.version_sg_tasks_versions"

        entity_reverse_regex = r"^sg_({fields})s(_\d+){{0,1}}$".format(
            fields="|".join([snake_case(e) for e in schema.entities])
        )

        cls._resolve_builtin_reverses(schema)

        # first, lets get all the reverse fields of multi_entity fields
        # since these are the easiest to identify
        cls._resolve_multi_entity_reverses(schema)

        # in order to best identify the reverse fields of entity fields,
        # (there's no *good* way), we're going to rely on the display names of fields,
        # since those are automatically generated by shotgun. The problem is that
        # if you rename a field, this breaks. First lets try to identify them based on
        # the display name of the field, but if we still have ambiguity, we'll need
        # to get all of the event logs for field display name changes and trace back to
        # the *original* display name and try again.
        # this sucks, but shotgun doesn't currently give us reverses in the schema :(

        unsolved_reverses: List[ShotgridField] = []
        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type != FieldType.MultiEntity:
                    continue
                if _field.reverse_of or _field.parent_of or _field.one_sided:
                    continue
                if not re.match(entity_reverse_regex, _field.api_name):
                    continue
                unsolved_reverses.append(_field)

        if unsolved_reverses:
            cls._resolve_entity_reverses(unsolved_reverses)

        unsolved_reverses = [
            f
            for f in unsolved_reverses
            if not f.reverse_of and not f.parent_of and not f.one_sided
        ]

        if not unsolved_reverses:
            return

        print(
            "Warning, some reverse fields could not be identified, "
            "scanning event logs for column rename events to help... "
            "this may take a while depending on how old your site is."
        )
        column_rename_events: List[dict] = sg.find(  # type: ignore
            "EventLogEntry",
            [
                ["event_type", "is", "Shotgun_DisplayColumn_Change"],
                ["attribute_name", "is", "human_name"],
            ],
            [
                "meta",
                "entity",
                "entity.DisplayColumn.entity_type",
            ],
        )
        assert column_rename_events

        cls._resolve_entity_reverses(unsolved_reverses, column_rename_events)

        unsolved_reverses = []
        for entity in schema.entities.values():
            if entity.api_name.endswith("Connection"):
                continue
            for _field in entity.fields.values():
                if _field.field_type not in [FieldType.MultiEntity, FieldType.Entity]:
                    continue
                if (
                    not _field.reverse_of
                    and not _field.parent_of
                    and not _field.one_sided
                ):
                    unsolved_reverses.append(_field)

    @classmethod
    def _resolve_multi_entity_reverses(cls, schema: Self) -> None:
        multi_entity_reverse_regex = r"^({entities})_(.+)_({entities})s$".format(
            entities="|".join([snake_case(e) for e in schema.entities])
        )
        for _, entity in schema.entities.items():
            for _, _field in entity.fields.items():
                if _field.field_type != FieldType.MultiEntity:
                    continue
                if _field.api_name == "asset_linked_projects_assets":
                    print("debug")
                match = re.match(multi_entity_reverse_regex, _field.api_name)
                if not match:
                    continue

                parent_entity_type = camel_case(match[1])
                parent_field_name = match[2]
                try:
                    parent_field = schema.entities[parent_entity_type].fields[
                        parent_field_name
                    ]
                except KeyError:
                    raise KeyError(
                        f"Field {parent_entity_type}.{parent_field_name} not found, "
                        "but is expected to be the parent of "
                        f"{entity.api_name}.{_field.api_name}"
                    )

                _field.reverse_of = parent_field
                if parent_field.parent_of:
                    parent_field.parent_of.append(_field)
                else:
                    parent_field.parent_of = [_field]

    @classmethod
    def _resolve_entity_reverses(
        cls,
        fields: List[ShotgridField],
        column_rename_events: Optional[List[Dict[str, Any]]] = None,
    ):
        column_original_display_names: Dict[str, str] = {}
        for event in sorted(column_rename_events or [], key=lambda x: x["id"]):
            if isinstance(event["meta"], list) or not event["entity"]:
                continue
            entity_type = event["entity.DisplayColumn.entity_type"]
            api_name = event["entity"]["name"]
            field_name = f"{entity_type}.{api_name}"
            if field_name not in column_original_display_names:
                old_Value = event["meta"]["old_value"]
                column_original_display_names[field_name] = old_Value

        for _field in fields:
            original_display_name = column_original_display_names.get(
                f"{_field.entity.api_name}.{_field.api_name}", _field.display_name
            )
            if "<->" not in original_display_name:
                continue
            parent_entity_type, parent_field_display_name = original_display_name.split(
                " <-> "
            )
            parent_entity_type = next(
                iter(
                    e.api_name
                    for e in _field.entity.schema.entities.values()
                    if e.display_name == parent_entity_type
                ),
                None,
            )
            if not parent_entity_type:
                continue

            matching_field = next(
                iter(
                    f
                    for f in _field.entity.schema.entities[
                        parent_entity_type
                    ].fields.values()
                    if column_original_display_names.get(
                        f"{f.entity.api_name}.{f.api_name}", f.display_name
                    )
                    == parent_field_display_name
                    and f.field_type == FieldType.Entity
                ),
                None,
            )
            if not matching_field:
                # try to see if there's only one possible reverse anyway...
                matching_fields = [
                    f
                    for f in _field.entity.schema.entities[
                        parent_entity_type
                    ].fields.values()
                    if f.field_type == FieldType.Entity
                    and _field.entity.api_name
                    in f.properties["properties"]["valid_types"]["value"]
                    and ((not f.reverse_of) and (not f.parent_of) and not (f.one_sided))
                ]
                if len(matching_fields) == 1:
                    matching_field = matching_fields[0]
                else:
                    continue

            _field.reverse_of = matching_field
            if isinstance(matching_field.parent_of, list):
                matching_field.parent_of.append(_field)
            else:
                matching_field.parent_of = [_field]

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
