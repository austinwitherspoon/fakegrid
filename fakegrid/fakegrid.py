from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union
from typing_extensions import Literal

from requests import get
from .schema import FieldType, ShotgridField, ShotgridSchema, camel_case, snake_case
from .models import (
    schema_to_models,
    Base,
    entity_sql_representation,
    multi_entity_sql_representation,
)
from sqlalchemy import (
    ClauseElement,
    and_,
    create_engine,
    Engine,
    literal_column,
    not_,
    or_,
    select,
)
from shotgun_api3 import Fault
from sqlalchemy.sql import functions
from sqlalchemy.orm import Session, load_only, aliased


class Fakegrid:
    _fakegrid_engine: Engine
    _fakegrid_schema: ShotgridSchema
    _fakegrid_models: Dict[str, Type[Base]]

    def __init__(
        self,
        base_url: Optional[str] = None,
        script_name: Optional[str] = None,
        api_key: Optional[str] = None,
        convert_datetimes_to_utc=True,
        http_proxy: Optional[str] = None,
        ensure_ascii=True,
        connect=True,
        ca_certs=None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        sudo_as_login: Optional[str] = None,
        session_token: Optional[str] = None,
        auth_token: Optional[str] = None,
    ):
        self._convert_datetimes_to_utc = convert_datetimes_to_utc

    @classmethod
    def from_schema(cls, schema: ShotgridSchema, engine: Optional[Engine] = None):
        models = schema_to_models(schema)
        engine = engine or create_engine("sqlite://:memory:")
        Base.metadata.create_all(engine)
        _class = cls()
        _class._fakegrid_engine = engine
        _class._fakegrid_schema = schema
        _class._fakegrid_models = models
        return _class

    def _session(self):
        return Session(self._fakegrid_engine)

    def create(
        self,
        entity_type: str,
        data: Dict[str, Any],
        return_fields: Optional[List[str]] = None,
        _session: Optional[Session] = None,
    ):
        model = self._fakegrid_models[entity_type]
        fields = set(data.keys()).union(set(return_fields or []))

        entity_names: Dict[str, Dict[int, Optional[str]]] = {}
        entity_dicts = []

        for field in fields:
            if isinstance(data[field], (dict, list)):
                entities = [
                    i
                    for i in (
                        [data[field]] if isinstance(data[field], dict) else data[field]
                    )
                    if "type" in i and "id" in i
                ]
                for entity in entities:
                    if entity["type"] not in entity_names:
                        entity_names[entity["type"]] = {}
                    entity_names[entity["type"]][entity["id"]] = None
                    entity_dicts.append(entity)

        is_temp_session = _session is None
        session = _session or self._session()
        # collect linked entity_names
        for entity_type, entity_ids in entity_names.items():
            if entity_type not in self._fakegrid_models:
                continue
            entity_model = self._fakegrid_models[entity_type]
            entity_name_field = entity_model._entity.name_field
            if not entity_name_field:
                continue
            query = (
                select(entity_model.id, getattr(entity_model, entity_name_field))
                .select_from(entity_model)
                .where(entity_model.id.in_(entity_ids.keys()))
            )
            for entity_id, entity_name in session.execute(query):
                entity_names[entity_type][entity_id] = entity_name

        # update data to include names
        for entity in entity_dicts:
            entity_name = entity_names[entity["type"]][entity["id"]]
            if entity_name:
                entity["name"] = entity_name

        instance = model.from_dict(data)
        session.add(instance)
        # create any multi-entity links
        for field in data:
            if field in ["type"]:
                continue
            field_schema = model._entity.fields[field]
            if field_schema.field_type == FieldType.MultiEntity:
                if not field_schema.connection_entity:
                    continue
                this_entity = instance.to_dict()
                connection_entity, connection_field = field_schema.connection_entity
                connection_model = self._fakegrid_models[connection_entity.api_name]
                connection_id_field = f"{connection_field}_id"
                connection_type_field = f"{connection_field}_type"
                existing_links = [
                    i[0]
                    for i in session.execute(
                        select(connection_model).where(
                            getattr(connection_model, connection_type_field)
                            == entity_type,
                            getattr(connection_model, connection_id_field)
                            == instance.id,
                            getattr(connection_model, "is_retired") == False,
                        )
                    )
                ]
                used_links = []
                new_links = data[field]

                for link in new_links:
                    link_type = link["type"]
                    link_id = link["id"]

                    link_field = snake_case(link_type)
                    if link_field not in connection_entity.fields:
                        if "linked_entity" in connection_entity.fields:
                            link_field = "linked_entity"
                        else:
                            link_field = next(
                                i
                                for i in connection_entity.fields.values()
                                if link_type
                                in i.properties["properties"]
                                .get("valid_types", {})
                                .get("value", [])
                            ).api_name
                    previous_existing_link: Optional[Base] = next(
                        iter(
                            i
                            for i in existing_links
                            if getattr(i, f"{link_field}_type") == link_type
                            and getattr(i, f"{link_field}_id") == link_id
                        ),
                        None,
                    )
                    if previous_existing_link:
                        used_links.append(previous_existing_link)
                        previous_existing_link.update_from_dict(
                            {
                                connection_field: this_entity,
                                link_field: {
                                    "type": link_type,
                                    "id": link_id,
                                    "name": entity_names[link_type][link_id],
                                },
                            }
                        )
                        continue

                    connection_instance = connection_model.from_dict(
                        {
                            connection_field: this_entity,
                            link_field: {
                                "type": link_type,
                                "id": link_id,
                                "name": entity_names[link_type][link_id],
                            },
                        }
                    )
                    session.add(connection_instance)
                to_delete = set(existing_links).difference(set(used_links))
                for i in to_delete:
                    session.delete(i)

        result = instance.to_dict(fields)
        if is_temp_session:
            session.commit()
            session.close()
        return result

    def find(
        self,
        entity_type: str,
        filters: list,
        fields: Optional[Iterable[str]] = None,
        order: Optional[Iterable[Dict[str, str]]] = None,
        filter_operator: Union[Literal["all"], Literal["any"], None] = None,
        limit=0,
        retired_only=False,
        page=0,
        include_archived_projects=True,
        additional_filter_presets=None,
    ):
        """
        Find entities matching the given filters.

            >>> # Find Character Assets in Sequence 100_FOO
            >>> # -------------
            >>> fields = ['id', 'code', 'sg_asset_type']
            >>> sequence_id = 2 # Sequence "100_FOO"
            >>> project_id = 4 # Demo Project
            >>> filters = [
            ...     ['project', 'is', {'type': 'Project', 'id': project_id}],
            ...     ['sg_asset_type', 'is', 'Character'],
            ...     ['sequences', 'is', {'type': 'Sequence', 'id': sequence_id}]
            ... ]
            >>> assets= sg.find("Asset",filters,fields)
            [{'code': 'Gopher', 'id': 32, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Cow', 'id': 33, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Bird_1', 'id': 35, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Bird_2', 'id': 36, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Bird_3', 'id': 37, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Raccoon', 'id': 45, 'sg_asset_type': 'Character', 'type': 'Asset'},
             {'code': 'Wet Gopher', 'id': 149, 'sg_asset_type': 'Character', 'type': 'Asset'}]

        You can drill through single entity links to filter on fields or display linked fields.
        This is often called "deep linking" or using "dot syntax".

            .. seealso:: :ref:`filter_syntax`

            >>> # Find Versions created by Tasks in the Animation Pipeline Step
            >>> # -------------
            >>> fields = ['id', 'code']
            >>> pipeline_step_id = 2 # Animation Step ID
            >>> project_id = 4 # Demo Project
            >>> # you can drill through single-entity link fields
            >>> filters = [
            ...     ['project','is', {'type': 'Project','id': project_id}],
            ...     ['sg_task.Task.step.Step.id', 'is', pipeline_step_id]
            >>> ]
            >>> sg.find("Version", filters, fields)
            [{'code': 'scene_010_anim_v001', 'id': 42, 'type': 'Version'},
             {'code': 'scene_010_anim_v002', 'id': 134, 'type': 'Version'},
             {'code': 'bird_v001', 'id': 137, 'type': 'Version'},
             {'code': 'birdAltBlue_v002', 'id': 236, 'type': 'Version'}]

        :param str entity_type: Shotgun entity type to find.
        :param list filters: list of filters to apply to the query.

            .. seealso:: :ref:`filter_syntax`

        :param list fields: Optional list of fields to include in each entity record returned.
            Defaults to ``["id"]``.
        :param list order: Optional list of dictionaries defining how to order the results of the
            query. Each dictionary contains the ``field_name`` to order by and  the ``direction``
            to sort::

                [{'field_name':'foo', 'direction':'asc'}, {'field_name':'bar', 'direction':'desc'}]

            Defaults to sorting by ``id`` in ascending order.
        :param str filter_operator: Operator to apply to the filters. Supported values are ``"all"``
            and ``"any"``. These are just another way of defining if the query is an AND or OR
            query. Defaults to ``"all"``.
        :param int limit: Optional limit to the number of entities to return. Defaults to ``0`` which
            returns all entities that match.
        :param int page: Optional page of results to return. Use this together with the ``limit``
            parameter to control how your query results are paged. Defaults to ``0`` which returns
            all entities that match.
        :param bool retired_only: Optional boolean when ``True`` will return only entities that have
            been retried. Defaults to ``False`` which returns only entities which have not been
            retired. There is no option to return both retired and non-retired entities in the
            same query.
        :param bool include_archived_projects: Optional boolean flag to include entities whose projects
            have been archived. Defaults to ``True``.
        :param additional_filter_presets: Optional list of presets to further filter the result
            set, list has the form::

                [{"preset_name": <preset_name>, <optional_param1>: <optional_value1>, ... }]

            Note that these filters are ANDed together and ANDed with the 'filter'
            argument.

            For details on supported presets and the format of this parameter see
            :ref:`additional_filter_presets`
        :returns: list of dictionaries representing each entity with the requested fields, and the
            defaults ``"id"`` and ``"type"`` which are always included.
        :rtype: list
        """
        model = self._fakegrid_models[entity_type]
        fields = fields or []

        _fields, _filters, joins = self._resolve_fields(model, fields, filters)

        wheres, new_joins = self._build_wheres_and_joins(entity_type, _filters)
        joins.extend(new_joins)
        print(_fields, _filters, joins)

        fields_to_query: List[Tuple[str, Any, Optional[ShotgridField]]] = [
            ("id", model.id, model._entity.fields["id"]),
            ("type", literal_column(f'"{model._entity.api_name}"'), None),
        ]  # type:ignore
        for alias_and_field in _fields:
            requested_field, alias, field_name = alias_and_field
            if requested_field in ["id", "type"]:
                continue
            actual_field_name = requested_field.split(".")[-1]
            actual_entity_type = (
                requested_field.split(".")[-2]
                if "." in requested_field
                else entity_type
            )
            field_schema = self._fakegrid_schema.entities[actual_entity_type].fields[
                actual_field_name
            ]
            if field_schema.field_type == FieldType.MultiEntity:
                fields_to_query.append(
                    (
                        requested_field,
                        multi_entity_sql_representation(
                            getattr(alias, f"{field_name}_type"),
                            getattr(alias, f"{field_name}_id"),
                            getattr(alias, f"{field_name}_name"),
                        ).label(requested_field),
                        field_schema,
                    )
                )
            else:
                fields_to_query.append(
                    (
                        requested_field,
                        getattr(alias, field_name).label(requested_field),
                        field_schema,
                    )
                )

        query = (
            select(*[i[1] for i in fields_to_query])
            .select_from(model)
            .group_by(model.id)
        )

        for join in joins:
            query = query.join(*join, isouter=True)
        for where in wheres:
            query = query.where(where)  # type:ignore

        if retired_only:
            query = query.where(getattr(model, "is_retired") == True)  # noqa
        else:
            query = query.where(getattr(model, "is_retired") == False)  # noqa

        if order:
            query = query.order_by(
                *[
                    getattr(getattr(model, i["field_name"]), i["direction"])()
                    for i in order
                ]
            )
        if limit:
            query = query.limit(limit)
        if page:
            query = query.offset(limit * page)

        transforms = []
        for requested_field, field_obj, field_schema in fields_to_query:
            if field_schema is None:
                transforms.append((requested_field, lambda x: x))
                continue
            transforms.append(
                (
                    requested_field,
                    lambda x, field_schema=field_schema: field_schema.from_database(x),
                )
            )

        with self._session() as session:
            print(query.compile(compile_kwargs={"literal_binds": True}))

            results = session.execute(query).all()

        return [
            {
                transforms[i][0]: transforms[i][1](field_value)
                for i, field_value in enumerate(result)
            }
            for result in results
        ]

    def _build_wheres_and_joins(
        self,
        entity_type: str,
        filters: Union[list, dict],
    ) -> Tuple[List[ClauseElement], List[Tuple[Type["Base"], Any]]]:
        wheres: List[ClauseElement] = []
        joins: List[Tuple[Type[Base], Any]] = []

        if not filters:
            return [], []

        if isinstance(filters, dict):
            group = []
            if not filters["filters"]:
                return [], []
            for sub_filter in filters["filters"]:
                group_wheres, group_joins = self._build_wheres_and_joins(
                    entity_type,
                    sub_filter,
                )
                group.extend(group_wheres)
                joins.extend(group_joins)
            if len(group) == 1:
                wheres.extend(group)
            else:
                if filters["filter_operator"] == "all":
                    wheres.append(and_(*group))
                elif filters["filter_operator"] == "any":
                    wheres.append(or_(*group))
                else:
                    raise NotImplementedError(
                        f"Unsupported filter_operator: {filters['filter_operator']}"
                    )
        # check if is a list of filters
        elif isinstance(filters, list) and all(
            isinstance(i, (list, dict)) for i in filters
        ):
            return self._build_wheres_and_joins(
                entity_type,
                {"filter_operator": "all", "filters": filters},
            )
        # otherwise we have a single filter!
        else:
            field_tuple, operator, *values = filters

            original, alias, field, parent_model = field_tuple

            field_schema = alias._entity.fields[field]
            values = [field_schema.to_database(i) for i in values]

            actual_field_name = original.split(".")[-1]
            actual_entity_type = (
                original.split(".")[-2] if "." in original else entity_type
            )
            actual_schema = self._fakegrid_schema.entities[actual_entity_type].fields[
                actual_field_name
            ]

            wheres.append(
                self._apply_operator(
                    field_schema, alias, operator, values, actual_schema, parent_model
                )
            )

        return wheres, joins

    """
    Operator                    Arguments
--------                    ---------
'is'                        [field_value] | None
'is_not'                    [field_value] | None
'less_than'                 [field_value] | None
'greater_than'              [field_value] | None
'contains'                  [field_value] | None
'not_contains'              [field_value] | None
'starts_with'               [string]
'ends_with'                 [string]
'between'                   [[field_value] | None, [field_value] | None]
'not_between'               [[field_value] | None, [field_value] | None]
'in_last'                   [[int], 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR']
                                   # note that brackets are not literal (eg. ['start_date', 'in_last', 1, 'DAY'])
'in_next'                   [[int], 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'YEAR']
                                   # note that brackets are not literal (eg. ['start_date', 'in_next', 1, 'DAY'])
'in'                        [[field_value] | None, ...] # Array of field values
'type_is'                   [string] | None             # Shotgun entity type
'type_is_not'               [string] | None             # Shotgun entity type
'in_calendar_day'           [int]                       # Offset (e.g. 0 = today, 1 = tomorrow,
                                                        # -1 = yesterday)
'in_calendar_week'          [int]                       # Offset (e.g. 0 = this week, 1 = next week,
                                                        # -1 = last week)
'in_calendar_month'         [int]                       # Offset (e.g. 0 = this month, 1 = next month,
                                                        # -1 = last month)
'name_contains'             [string]
'name_not_contains'         [string]
'name_starts_with'          [string]
'name_ends_with'            [string]
    """

    def _apply_operator(
        self,
        field_schema: ShotgridField,
        model: Type[Base],
        operator: str,
        value: Any,
        actual_schema: Optional[ShotgridField] = None,
        parent_model: Optional[Type[Base]] = None,
    ) -> ClauseElement:
        actual_schema = actual_schema or field_schema
        parent_model = parent_model or model
        field = getattr(model, field_schema.api_name)
        if actual_schema.field_type == FieldType.Entity:
            id_field = getattr(model, f"{actual_schema.api_name}_id")
            type_field = getattr(model, f"{actual_schema.api_name}_type")
            name_field = getattr(model, f"{actual_schema.api_name}_name")
            if operator == "is":
                return and_(
                    id_field == value[0]["id"],
                    type_field == value[0]["type"],
                )
            elif operator == "is_not":
                return or_(
                    id_field != value[0]["id"],
                    type_field != value[0]["type"],
                )
            elif operator == "type_is":
                return type_field == value[0]
            elif operator == "type_is_not":
                return type_field != value[0]
            elif operator == "name_contains":
                return name_field.like(f"%{value[0]}%")
            elif operator == "name_not_contains":
                return name_field.notlike(f"%{value[0]}%")
            elif operator == "name_is":
                return name_field == value[0]
            elif operator == "in":
                return or_(
                    *[
                        and_(
                            id_field == i["id"],
                            type_field == i["type"],
                        )
                        for i in value
                    ]
                )
            elif operator == "not_in":
                return and_(
                    *[
                        or_(
                            id_field != i["id"],
                            type_field != i["type"],
                        )
                        for i in value
                    ]
                )
        elif actual_schema.field_type in [
            FieldType.MultiEntity,
            FieldType.Addressing,
            FieldType.TagList,
        ]:
            id_field = getattr(model, f"{field_schema.api_name}_id")
            type_field = getattr(model, f"{field_schema.api_name}_type")
            name_field = getattr(model, f"{field_schema.api_name}_name")
            connection_field = actual_schema.connection_entity[1]
            raw_connection_model = self._fakegrid_models[
                actual_schema.connection_entity[0].api_name
            ]

            if isinstance(value[0], list) and operator in ["is", "is_not"]:
                raise Fault(
                    f"API read() '{operator}' 'relation' expects a 1-element array:\n"
                    f"{value[0]}"
                )

            if operator == "is":
                return and_(
                    id_field == value[0]["id"],
                    type_field == value[0]["type"],
                )
            elif operator == "is_not":
                return not_(
                    select(getattr(raw_connection_model, "id"))
                    .select_from(raw_connection_model)
                    .where(
                        and_(
                            getattr(raw_connection_model, f"{field_schema.api_name}_id")
                            == value[0]["id"],
                            getattr(
                                raw_connection_model,
                                f"{field_schema.api_name}_type",
                            )
                            == value[0]["type"],
                            getattr(raw_connection_model, f"{connection_field}_id")
                            == getattr(parent_model, "id"),
                            getattr(raw_connection_model, f"{connection_field}_type")
                            == parent_model._entity.api_name,
                            getattr(raw_connection_model, "is_retired") == False,
                        )
                    )
                    .exists()
                )
            elif operator == "type_is":
                return type_field == value[0]
            elif operator == "type_is_not":
                return not_(
                    select(getattr(raw_connection_model, "id"))
                    .select_from(raw_connection_model)
                    .where(
                        and_(
                            getattr(
                                raw_connection_model,
                                f"{field_schema.api_name}_type",
                            )
                            == value[0],
                            getattr(raw_connection_model, f"{connection_field}_id")
                            == getattr(parent_model, "id"),
                            getattr(raw_connection_model, f"{connection_field}_type")
                            == parent_model._entity.api_name,
                            getattr(raw_connection_model, "is_retired") == False,
                        )
                    )
                    .exists()
                )
            elif operator == "name_contains":
                return name_field.like(f"%{value[0]}%")
            elif operator == "name_not_contains":
                return not_(
                    select(getattr(raw_connection_model, "id"))
                    .select_from(raw_connection_model)
                    .where(
                        and_(
                            getattr(
                                raw_connection_model, f"{field_schema.api_name}_name"
                            ).like(f"%{value[0]}%"),
                            getattr(raw_connection_model, f"{connection_field}_id")
                            == getattr(parent_model, "id"),
                            getattr(raw_connection_model, f"{connection_field}_type")
                            == parent_model._entity.api_name,
                            getattr(raw_connection_model, "is_retired") == False,
                        )
                    )
                    .exists()
                )
            elif operator == "name_is":
                return name_field == value[0]
            elif operator == "in":
                return or_(
                    *[
                        and_(
                            id_field == i["id"],
                            type_field == i["type"],
                        )
                        for i in value[0]
                    ]
                )
            elif operator == "not_in":
                return not_(
                    select(getattr(raw_connection_model, "id"))
                    .select_from(raw_connection_model)
                    .where(
                        and_(
                            or_(
                                *[
                                    and_(
                                        getattr(
                                            raw_connection_model,
                                            f"{field_schema.api_name}_id",
                                        )
                                        == i["id"],
                                        getattr(
                                            raw_connection_model,
                                            f"{field_schema.api_name}_type",
                                        )
                                        == i["type"],
                                    )
                                    for i in value[0]
                                ]
                            ),
                            getattr(raw_connection_model, f"{connection_field}_id")
                            == getattr(parent_model, "id"),
                            getattr(raw_connection_model, f"{connection_field}_type")
                            == parent_model._entity.api_name,
                            getattr(raw_connection_model, "is_retired") == False,
                        )
                    )
                    .exists()
                )

        else:
            if operator == "is":
                return field == value[0]
            elif operator == "is_not":
                return field != value[0]
            elif operator == "less_than":
                return field < value[0]
            elif operator == "greater_than":
                return field > value[0]
            elif operator == "contains":
                return field.like(f"%{value[0]}%")
            elif operator == "not_contains":
                return field.notlike(f"%{value[0]}%")
            elif operator == "starts_with":
                return field.like(f"{value[0]}%")
            elif operator == "ends_with":
                return field.like(f"%{value[0]}")
            elif operator == "between":
                return field.between(value[0], value[1])
            elif operator == "not_between":
                return field.notbetween(value[0], value[1])
            elif operator == "in":
                return or_(*[field == i for i in value[0]])
            elif operator == "not_in":
                return and_(*[field != i for i in value[0]])
            else:
                raise NotImplementedError(f"Unsupported operator: {operator}")

    def _resolve_fields(
        self,
        model: Type[Base],
        fields: Iterable[str],
        filters: list,
        _joins=None,
    ) -> Tuple[
        List[Tuple[str, Type[Base], str]],
        list,
        List[Tuple[Type["Base"], Any]],
    ]:
        """replace all fields with instances of the field on the model
        (or alias if a dotted field.)

        Returns a new list of fields, a new list of filters, and a list of joins required to get the fields.
        """
        new_fields = []
        new_filters = []
        joins = _joins if _joins is not None else []
        for field in fields:
            original, alias, field, _ = self._resolve_dotted_field(model, field, joins)
            new_fields.append((original, alias, field))
        for filter in filters:
            if isinstance(filter, dict):
                new_filters.append(
                    {
                        "filter_operator": filter["filter_operator"],
                        "filters": self._resolve_fields(model, [], filters, joins)[1],
                    }
                )
            else:
                field = filter[0]
                original, alias, field, parent_model = self._resolve_dotted_field(
                    model, field, joins
                )
                new_filters.append(
                    [(original, alias, field, parent_model), *filter[1:]]
                )
        return new_fields, new_filters, joins

    def _resolve_dotted_field(
        self,
        model: Type[Base],
        field: str,
        joins: List[Tuple[Type["Base"], Any]],
    ) -> Tuple[str, Type[Base], str, Type[Base]]:
        original_field = field
        parent_schema = model._entity
        previous_alias = model
        parent_alias = model
        is_multi_entity = parent_schema.fields[field].field_type in [
            FieldType.MultiEntity,
            FieldType.Addressing,
            FieldType.TagList,
        ]
        if "." not in field and not is_multi_entity:
            return field, model, field, model
        if "." in field:
            field_layers = field.split(".")
            for i in range(0, len(field_layers), 2):
                field = field_layers[i]
                field_entity_name = (
                    field_layers[i + 1] if i + 1 < len(field_layers) else None
                )
                field_schema = parent_schema.fields[field]
                if not field_entity_name:
                    continue
                if field_schema.field_type == FieldType.Entity:
                    new_alias = aliased(self._fakegrid_models[field_entity_name])
                    joins.append(
                        (
                            new_alias,
                            and_(
                                getattr(previous_alias, f"{field}_id")
                                == new_alias.id,  # type:ignore
                                getattr(previous_alias, f"{field}_type")
                                == field_entity_name,
                            ),
                        )
                    )
                    parent_alias = previous_alias
                    previous_alias = new_alias
                elif field_schema.field_type == FieldType.MultiEntity:
                    # join connection table
                    if field_schema.connection_entity:
                        connection_entity, conn_field = field_schema.connection_entity
                        connection_model = self._fakegrid_models[
                            connection_entity.api_name
                        ]
                        conn_opposite_field = "linked_entity"
                        if conn_opposite_field not in connection_entity.fields:
                            conn_opposite_field = snake_case(field_entity_name)
                        if conn_opposite_field not in connection_entity.fields:
                            conn_opposite_field = next(
                                i
                                for i in connection_entity.fields.values()
                                if field_entity_name
                                in i.properties["properties"]
                                .get("valid_types", {})
                                .get("value", [])
                            ).api_name

                        new_alias = aliased(connection_model)
                        joins.append(
                            (
                                new_alias,
                                and_(
                                    getattr(previous_alias, "id")
                                    == getattr(new_alias, f"{conn_field}_id"),
                                    getattr(new_alias, f"{conn_field}_type")
                                    == parent_schema.api_name,
                                    getattr(new_alias, "is_retired") == False,  # noqa
                                ),
                            )
                        )
                        conn_alias = new_alias
                        new_alias = aliased(self._fakegrid_models[field_entity_name])
                        joins.append(
                            (
                                new_alias,
                                and_(
                                    getattr(new_alias, "id")
                                    == getattr(conn_alias, f"{conn_opposite_field}_id"),
                                    getattr(conn_alias, f"{conn_opposite_field}_type")
                                    == field_entity_name,
                                    getattr(conn_alias, "is_retired") == False,  # noqa
                                ),
                            )
                        )
                        parent_alias = previous_alias
                        previous_alias = new_alias
                    else:
                        reverse_field = field_schema.reverse_of
                        assert reverse_field
                        reverse_entity = reverse_field.entity
                        reverse_model = self._fakegrid_models[reverse_entity.api_name]
                        new_alias = aliased(reverse_model)
                        joins.append(
                            (
                                new_alias,
                                and_(
                                    getattr(previous_alias, "id")
                                    == getattr(
                                        new_alias, f"{reverse_field.api_name}_id"
                                    ),
                                    getattr(new_alias, f"{reverse_field.api_name}_type")
                                    == parent_schema.api_name,
                                    getattr(new_alias, "is_retired") == False,  # noqa
                                ),
                            )
                        )
                        parent_alias = previous_alias
                        previous_alias = new_alias

                parent_schema = self._fakegrid_schema.entities[field_entity_name]
        elif is_multi_entity:
            field_schema = parent_schema.fields[field]
            if field_schema.connection_entity:
                connection_entity, conn_field = field_schema.connection_entity
                connection_model = self._fakegrid_models[connection_entity.api_name]
                conn_opposite_field = "linked_entity"
                if conn_opposite_field not in connection_entity.fields:
                    conn_opposite_field = snake_case(original_field)
                if conn_opposite_field not in connection_entity.fields:
                    conn_opposite_field = next(
                        i
                        for i in connection_entity.fields.values()
                        if original_field
                        in i.properties["properties"]
                        .get("valid_types", {})
                        .get("value", [])
                    ).api_name

                new_alias = aliased(connection_model)
                joins.append(
                    (
                        new_alias,
                        and_(
                            getattr(previous_alias, "id")
                            == getattr(new_alias, f"{conn_field}_id"),
                            getattr(new_alias, f"{conn_field}_type")
                            == parent_schema.api_name,
                            getattr(new_alias, "is_retired") == False,  # noqa
                        ),
                    )
                )
                parent_alias = previous_alias
                previous_alias = new_alias
                field = conn_opposite_field

        return original_field, previous_alias, field, parent_alias
