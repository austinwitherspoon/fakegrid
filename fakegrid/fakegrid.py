import calendar
import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union
from typing_extensions import Literal

from requests import get
from .schema import (
    FieldType,
    ShotgridField,
    ShotgridSchema,
    camel_case,
    snake_case,
    ALLOWED_OPERATIONS_BY_FIELD_TYPE,
)
from .models import (
    schema_to_models,
    Base,
    entity_sql_representation,
    multi_entity_sql_representation,
)
from sqlalchemy import (
    ClauseElement,
    and_,
    case,
    create_engine,
    Engine,
    literal_column,
    not_,
    or_,
    select,
    text,
)
from shotgun_api3 import Fault, sg_timezone
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
        data.pop("type", None)
        fields = set(data.keys()).union(set(return_fields or []))

        # correct UTC dates
        if self._convert_datetimes_to_utc:
            for field in fields:
                if field not in data:
                    continue
                field_schema = model._entity.fields[field]
                if field_schema.field_type == FieldType.DateTime:
                    local_time = data[field]
                    if not local_time:
                        continue
                    if local_time.tzinfo is None:
                        local_time.replace(tzinfo=sg_timezone.local)
                    utc_time = local_time.astimezone(sg_timezone.utc)
                    data[field] = utc_time

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
        if entity_type not in self._fakegrid_models:
            raise Fault(f'API read() invalid entity type "{entity_type}"')
        model = self._fakegrid_models[entity_type]
        fields = fields or []

        _fields, _filters, joins = self._resolve_fields(model, fields, [])

        wheres, new_joins = self._build_wheres_and_joins(entity_type, filters)
        joins.extend(new_joins)
        print(_fields, _filters, joins)

        fields_to_query: List[Tuple[str, Any, Optional[ShotgridField]]] = [
            ("id", getattr(model, "id"), model._entity.fields["id"]),
            ("type", literal_column(f"'{model._entity.api_name}'").label("type"), None),
        ]  # type:ignore
        for alias_and_field in _fields:
            requested_field, alias, field_name, parent_model = alias_and_field
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
                if (
                    field_name == "id"
                ):  # this is entity reverse field! Get the actual entities
                    entity_schema = alias._entity
                    name_field = entity_schema.name_field
                    column = case(
                        {getattr(parent_model, "id") == None: None},
                        else_=multi_entity_sql_representation(
                            entity_schema.api_name,
                            getattr(alias, "id"),
                            getattr(alias, name_field) if name_field else None,
                        ),
                    ).label(requested_field)
                else:
                    column = case(
                        {getattr(parent_model, "id") == None: None},
                        else_=multi_entity_sql_representation(
                            getattr(alias, f"{field_name}_type"),
                            getattr(alias, f"{field_name}_id"),
                            getattr(alias, f"{field_name}_name"),
                        ),
                    ).label(requested_field)

                fields_to_query.append(
                    (
                        requested_field,
                        column,
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

        query = query.order_by(
            *[
                getattr(getattr(model, i["field_name"]), i["direction"])()
                for i in order or [{"field_name": "id", "direction": "asc"}]
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
            transform_function = lambda x, schema=field_schema: schema.from_database(x)
            if field_schema.field_type == FieldType.DateTime:
                if self._convert_datetimes_to_utc:
                    # convert server (UTC) to local timezone
                    transform_function = (
                        lambda x, func=transform_function: sg_timezone.local.fromutc(
                            func(x)
                        )
                        if x
                        else x
                    )
                    # transform_function = lambda x: x.replace(tzinfo=sg_timezone.local)
            if field_schema.field_type == FieldType.MultiEntity:
                transform_function = (
                    lambda x, func=transform_function: sorted(
                        func(x), key=lambda i: i["name"].lower()
                    )
                    if x
                    else x
                )
            transforms.append(
                (
                    requested_field,
                    transform_function,
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
            field, operator, *values = filters

            # field_schema = alias._entity.fields[field]
            # if isinstance(values[0], list):
            #     values = [[field_schema.to_database(i) for i in v] for v in values]
            # else:
            #     values = [field_schema.to_database(i) for i in values]

            # actual_field_name = original.split(".")[-1]
            # actual_entity_type = (
            #     original.split(".")[-2] if "." in original else entity_type
            # )
            # actual_schema = self._fakegrid_schema.entities[actual_entity_type].fields[
            #     actual_field_name
            # ]
            parent_model = self._fakegrid_models[entity_type]

            wheres.append(self._apply_operator(field, parent_model, operator, values))

        return wheres, joins

    def _apply_operator(
        self,
        field: str,
        model: Type[Base],
        operator: str,
        value: Any,
    ) -> ClauseElement:
        negative_operators = {
            "is_not": "is",
            "not_in": "in",
            "not_contains": "contains",
            "not_between": "between",
            "type_is_not": "type_is",
            "name_not_contains": "name_contains",
            "not_in_next": "in_next",
            "not_in_last": "in_last",
        }
        if operator in negative_operators:
            operator = negative_operators[operator]
            return not_(
                self._apply_operator(field, model, operator, value)
            )  # type:ignore

        if "." in field:
            field_parts = field.split(".")
            nested_fields = [
                (
                    field_parts[i],
                    field_parts[i + 1] if len(field_parts) > i + 1 else None,
                )
                for i in range(0, len(field_parts), 2)
            ]
            next_field, next_entity_type = next(iter(nested_fields))

            if next_entity_type:
                try:
                    next_model = self._fakegrid_models[next_entity_type]
                except KeyError:
                    raise Fault(
                        f"API read() {model._entity.api_name}.{next_field} is not a valid relation."
                    )

                if not model._entity.fields.get(next_field, None):
                    raise Fault(
                        f"API read() {model._entity.api_name}.{next_field} doesn't exist."
                    )
                if model._entity.fields[next_field].field_type not in [
                    FieldType.Entity,
                    FieldType.MultiEntity,
                    FieldType.Addressing,
                    FieldType.TagList,
                ]:
                    raise Fault(
                        f"API read() {model._entity.api_name}.{field} is not a valid relation."
                    )

                if model._entity.fields[next_field].field_type in [
                    FieldType.MultiEntity,
                    FieldType.Addressing,
                    FieldType.TagList,
                ]:
                    connection_entity = model._entity.fields[
                        next_field
                    ].connection_entity or next(
                        iter(
                            (i.entity, i.api_name)
                            for i in model._entity.fields[next_field].parent_of or []
                            if i.entity.api_name == next_entity_type
                        ),
                        None,
                    )
                    if not connection_entity:
                        raise Fault(
                            f"API read() {model._entity.api_name}.{field} is not a valid relation."
                        )
                    conn_model = self._fakegrid_models[connection_entity[0].api_name]
                    conn_alias = aliased(conn_model)
                    conn_field = conn_model._entity.fields[connection_entity[1]]
                    opposite_field = conn_field.connection_query_target_field

                    field = ".".join(
                        ".".join([i for i in f if i]) for f in nested_fields[1:]
                    )
                    if opposite_field:
                        field = f"{opposite_field.api_name}.{next_entity_type}.{field}"

                    inner_where = self._apply_operator(
                        field, conn_alias, operator, value
                    )
                    origin_model_id = getattr(model, "id")
                    conn_model_id = getattr(conn_alias, f"{conn_field.api_name}_id")
                    conn_model_type = getattr(conn_alias, f"{conn_field.api_name}_type")

                    outer_where = (
                        select(text("NULL"))
                        .select_from(conn_alias)
                        .where(
                            and_(
                                conn_model_id == origin_model_id,
                                conn_model_type == model._entity.api_name,
                                inner_where,
                            )
                        )
                        .exists()
                    )
                    return outer_where

                # handle single entity link
                alias = aliased(next_model)

                # reconstruct the field, minus the one we're currently deconstructing
                field = ".".join(
                    ".".join([i for i in f if i]) for f in nested_fields[1:]
                )
                inner_where = self._apply_operator(field, alias, operator, value)

                id_field = getattr(model, f"{next_field}_id")
                type_field = getattr(model, f"{next_field}_type")

                outer_where = and_(
                    select(text("NULL"))
                    .select_from(alias)
                    .where(
                        and_(
                            id_field == getattr(alias, "id"),
                            inner_where,
                        )
                    )
                    .exists(),
                    type_field == next_entity_type,
                )

                return outer_where

            else:
                where = self._apply_operator(next_field, model, operator, value)
                return where

        field_schema = model._entity.fields.get(field)
        if not field_schema and field != "{self}":
            raise Fault(f"API read() {model._entity.api_name}.{field} doesn't exist.")

        if field_schema and field_schema.field_type in [
            FieldType.MultiEntity,
            FieldType.Addressing,
            FieldType.TagList,
        ]:
            connection_entity = field_schema.connection_entity or (
                (field_schema.reverse_of.entity, field_schema.reverse_of.api_name)
                if field_schema.reverse_of
                else None
            )
            if not connection_entity:
                raise Fault(
                    f"API read() {model._entity.api_name}.{field} is not a valid relation."
                )
            conn_model = self._fakegrid_models[connection_entity[0].api_name]
            conn_alias = aliased(conn_model)
            conn_field = conn_model._entity.fields[connection_entity[1]]
            opposite_field = conn_field.connection_query_target_field

            if opposite_field:
                inner_where = self._apply_operator(
                    opposite_field.api_name, conn_alias, operator, value
                )
            else:
                inner_where = self._apply_operator(
                    "{self}", conn_alias, operator, value
                )

            origin_model_id = getattr(model, "id")
            conn_model_id = getattr(
                conn_alias, f"{conn_field.api_name}_id", getattr(conn_alias, "id")
            )
            conn_model_type = getattr(
                conn_alias,
                f"{conn_field.api_name}_type",
                literal_column(f"'{conn_field.entity.api_name}'"),
            )

            where = (
                select(text("NULL"))
                .select_from(conn_alias)
                .where(
                    and_(
                        conn_model_id == origin_model_id,
                        conn_model_type == model._entity.api_name,
                        inner_where,
                    )
                )
                .exists()
            )
            return where

        sql_field = getattr(model, field_schema.api_name) if field_schema else None
        field_type = field_schema.field_type if field_schema else FieldType.Entity
        entity_name = (
            field_schema.entity.api_name if field_schema else model._entity.api_name
        )
        valid_operators = ALLOWED_OPERATIONS_BY_FIELD_TYPE[field_type]
        if operator not in valid_operators:
            raise Fault(
                f"API read() {entity_name}.{field}'s "
                f"'{field_type.value}' data type doesn't support "
                f"'{operator}' 'relation':\n"
                f"Valid relations are: [{', '.join(valid_operators)}]"
            )

        if not isinstance(value[0], list) and operator in ["in", "not_in"]:
            operator = {"in": "is", "not_in": "is_not"}[operator]

        value = field_schema.to_database(value) if field_schema else value

        if field_type == FieldType.Entity:
            if field_schema:
                id_field = getattr(model, f"{field_schema.api_name}_id")
                type_field = getattr(model, f"{field_schema.api_name}_type")
                name_field = getattr(model, f"{field_schema.api_name}_name")
            else:
                id_field = getattr(model, "id")
                type_field = literal_column(f"'{model._entity.api_name}'")
                name_field = (
                    getattr(model, model._entity.name_field)
                    if model._entity.name_field
                    else literal_column("NULL")
                )
            if operator == "is":
                if value[0] is None:
                    return and_(id_field == None, type_field == None)
                return and_(
                    id_field == value[0]["id"],
                    type_field == value[0]["type"],
                    id_field != None,
                    type_field != None,
                )
            elif operator == "type_is":
                if value[0] is None:
                    return type_field == None
                return type_field == value[0]
            elif operator == "name_contains":
                return and_(name_field.like(f"%{value[0]}%"), name_field != None)
            elif operator == "name_is":
                return and_(name_field == value[0], name_field != None)
            elif operator == "in":
                return and_(
                    or_(
                        *[
                            and_(
                                id_field == i["id"],
                                type_field == i["type"],
                            )
                            if i
                            else and_(id_field == None, type_field == None)
                            for i in value[0]
                        ]
                    ),
                    id_field != None,
                    type_field != None,
                )
        else:
            assert sql_field
            if operator == "is":
                return and_(sql_field == value[0], sql_field != None)
            elif operator == "in":
                return and_(or_(*[sql_field == i for i in value[0]]), sql_field != None)
            elif operator == "less_than":
                return and_(sql_field < value[0], sql_field != None)
            elif operator == "greater_than":
                return and_(sql_field > value[0], sql_field != None)
            elif operator == "contains":
                return and_(sql_field.like(f"%{value[0]}%"), sql_field != None)
            elif operator == "starts_with":
                return and_(sql_field.like(f"{value[0]}%"), sql_field != None)
            elif operator == "ends_with":
                return and_(sql_field.like(f"%{value[0]}"), sql_field != None)
            elif operator == "between":
                if isinstance(value[0], list):
                    value = value[0]
                return and_(sql_field.between(value[0], value[1]), sql_field != None)
            elif operator in ["in_last"]:
                amount, unit = value
                if not isinstance(amount, (int, float)):
                    raise Fault(
                        f"API read() 'in_last' 'relation' expects an integer:\n{amount}"
                    )
                if unit not in ["HOUR", "DAY", "WEEK", "MONTH", "YEAR"]:
                    raise Fault(
                        f"API read() 'in_last' 'relation' expects a unit of time:\n{unit}"
                    )
                # convert to hours
                hours = amount
                if unit == "DAY":
                    hours = amount * 24
                elif unit == "WEEK":
                    hours = amount * 24 * 7
                elif unit == "MONTH":
                    hours = amount * 24 * 30
                elif unit == "YEAR":
                    hours = amount * 24 * 365
                return and_(
                    sql_field
                    >= datetime.datetime.utcnow() - datetime.timedelta(hours=hours),
                    sql_field <= datetime.datetime.utcnow(),
                    sql_field != None,
                )
            elif operator in ["in_next"]:
                amount, unit = value
                if not isinstance(amount, (int, float)):
                    raise Fault(
                        f"API read() 'in_last' 'relation' expects an integer:\n{amount}"
                    )
                if unit not in ["HOUR", "DAY", "WEEK", "MONTH", "YEAR"]:
                    raise Fault(
                        f"API read() 'in_last' 'relation' expects a unit of time:\n{unit}"
                    )
                # convert to hours
                hours = amount
                if unit == "DAY":
                    hours = amount * 24
                elif unit == "WEEK":
                    hours = amount * 24 * 7
                elif unit == "MONTH":
                    hours = amount * 24 * 30
                elif unit == "YEAR":
                    hours = amount * 24 * 365

                return and_(
                    sql_field
                    <= datetime.datetime.utcnow() + datetime.timedelta(hours=hours),
                    sql_field >= datetime.datetime.utcnow(),
                    sql_field != None,
                )
            elif operator == "in_calendar_day":
                amount = value[0]
                if not isinstance(amount, (int, float)):
                    raise Fault(
                        f"API read() 'in_calendar_day' 'relation' expects an integer:\n{amount}"
                    )
                # offset today by amount days
                day = datetime.datetime.utcnow() + datetime.timedelta(days=amount)
                # get the start of the day
                start_of_day = day.replace(hour=0, minute=0, second=0, microsecond=0)
                # get the end of the day
                end_of_day = day.replace(
                    hour=23, minute=59, second=59, microsecond=999999
                )
                return and_(
                    sql_field.between(start_of_day, end_of_day), sql_field != None
                )
            elif operator == "in_calendar_week":
                amount = value[0]
                if not isinstance(amount, (int, float)):
                    raise Fault(
                        f"API read() 'in_calendar_week' 'relation' expects an integer:\n{amount}"
                    )
                # offset today by amount weeks
                week = datetime.datetime.utcnow() + datetime.timedelta(weeks=amount)
                week = week.replace(hour=0, minute=0, second=0, microsecond=0)
                # get the start of the week
                start_of_week = week - datetime.timedelta(days=week.weekday())
                # get the end of the week
                end_of_week = start_of_week + datetime.timedelta(days=6)
                return and_(
                    sql_field.between(start_of_week, end_of_week), sql_field != None
                )
            elif operator == "in_calendar_month":
                amount = value[0]
                if not isinstance(amount, (int, float)):
                    raise Fault(
                        f"API read() 'in_calendar_month' 'relation' expects an integer:\n{amount}"
                    )
                # offset today by amount months
                current_month = datetime.datetime.utcnow().month
                month = int(current_month + amount)
                # get the start of the month
                start_of_month = datetime.datetime.utcnow().replace(
                    month=month, day=1, hour=0, minute=0, second=0, microsecond=0
                )
                # get the end of the month
                end_of_month = datetime.datetime.utcnow().replace(
                    month=month,
                    day=calendar.monthrange(month, month)[1],
                    hour=23,
                    minute=59,
                    second=59,
                    microsecond=999999,
                )
                return and_(
                    sql_field.between(start_of_month, end_of_month), sql_field != None
                )

            else:
                raise NotImplementedError(f"Unsupported operator: {operator}")

    def _resolve_fields(
        self,
        model: Type[Base],
        fields: Iterable[str],
        filters: list,
        _joins=None,
    ) -> Tuple[
        List[Tuple[str, Type[Base], str, Type[Base]]],
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
            try:
                original, alias, field, parent_model = self._resolve_dotted_field(
                    model, field, joins
                )
                new_fields.append((original, alias, field, parent_model))
            except Fault:
                continue
        for filter in filters:
            if isinstance(filter, dict):
                new_filters.append(
                    {
                        "filter_operator": filter["filter_operator"],
                        "filters": self._resolve_fields(
                            model, [], filter["filters"], joins
                        )[1],
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
        dotted_entity = (
            field.split(".")[-2]
            if len(field.split(".")) > 1
            else parent_schema.api_name
        )
        dotted_field = field.split(".")[-1] if len(field.split(".")) > 1 else field
        if dotted_field not in self._fakegrid_schema.entities[dotted_entity].fields:
            raise Fault(f"API read() {model._entity.api_name}.{field} doesn't exist.")
        is_multi_entity = self._fakegrid_schema.entities[dotted_entity].fields[
            dotted_field
        ].field_type in [
            FieldType.MultiEntity,
            FieldType.Addressing,
            FieldType.TagList,
        ]
        if "." not in field and not is_multi_entity:
            return field, model, field, model
        field_layers = field.split(".")
        for i in range(0, len(field_layers), 2):
            field = field_layers[i]
            field_entity_name = (
                field_layers[i + 1] if i + 1 < len(field_layers) else None
            )
            field_schema = parent_schema.fields[field]
            field_entity_name = field_entity_name or previous_alias._entity.api_name
            if field_schema.field_type == FieldType.Entity:
                new_alias = aliased(self._fakegrid_models[field_entity_name])
                new_join = (
                    new_alias,
                    and_(
                        getattr(previous_alias, f"{field}_id")
                        == new_alias.id,  # type:ignore
                        getattr(previous_alias, f"{field}_type") == field_entity_name,
                    ),
                )
                if i + 2 < len(field_layers):
                    parent_alias = previous_alias
                    previous_alias = new_alias
                    joins.append(new_join)
            elif field_schema.field_type == FieldType.MultiEntity:
                # join connection table
                if field_schema.connection_entity:
                    connection_entity, conn_field_name = field_schema.connection_entity
                    connection_model = self._fakegrid_models[connection_entity.api_name]
                    conn_field = connection_entity.fields[conn_field_name]
                    assert conn_field
                    conn_opposite_field = conn_field.connection_query_target_field
                    assert conn_opposite_field
                    conn_opposite_field_name = conn_opposite_field.api_name

                    new_alias = aliased(connection_model)
                    joins.append(
                        (
                            new_alias,
                            and_(
                                getattr(previous_alias, "id")
                                == getattr(new_alias, f"{conn_field_name}_id"),
                                getattr(new_alias, f"{conn_field_name}_type")
                                == parent_schema.api_name,
                                getattr(new_alias, "is_retired") == False,  # noqa
                            ),
                        )
                    )
                    conn_alias = new_alias
                    new_alias = aliased(self._fakegrid_models[field_entity_name])
                    new_join = (
                        new_alias,
                        and_(
                            getattr(new_alias, "id")
                            == getattr(conn_alias, f"{conn_opposite_field_name}_id"),
                            getattr(conn_alias, f"{conn_opposite_field_name}_type")
                            == field_entity_name,
                            getattr(conn_alias, "is_retired") == False,  # noqa
                        ),
                    )
                    parent_alias = previous_alias
                    previous_alias = new_alias
                    field = conn_opposite_field_name
                    # don't join new table if we're not drilling down further
                    if i + 2 < len(field_layers):
                        joins.append(new_join)
                    else:
                        previous_alias = conn_alias
                else:
                    reverse_field = field_schema.reverse_of
                    assert reverse_field
                    reverse_entity = reverse_field.entity
                    reverse_model = self._fakegrid_models[reverse_entity.api_name]
                    new_alias = aliased(reverse_model)
                    new_join = (
                        new_alias,
                        and_(
                            getattr(previous_alias, "id")
                            == getattr(new_alias, f"{reverse_field.api_name}_id"),
                            getattr(new_alias, f"{reverse_field.api_name}_type")
                            == parent_schema.api_name,
                            getattr(new_alias, "is_retired") == False,  # noqa
                        ),
                    )
                    parent_alias = previous_alias
                    previous_alias = new_alias
                    field = "id"
                    joins.append(new_join)

            parent_schema = self._fakegrid_schema.entities[field_entity_name]

        return original_field, previous_alias, field, parent_alias
