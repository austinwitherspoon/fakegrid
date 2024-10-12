from enum import Enum


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


ALLOWED_OPERATIONS_BY_FIELD_TYPE = {
    FieldType.TEXT: [
        "is",
        "is_not",
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
        "in",
        "not_in",
    ],
    FieldType.FLOAT: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.MULTI_ENTITY: [
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
    FieldType.NUMBER: [
        "is",
        "is_not",
        "less_than",
        "greater_than",
        "between",
        "not_between",
        "in",
        "not_in",
    ],
    FieldType.ADDRESSING: [
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
    FieldType.CHECKBOX: ["is", "is_not"],
    FieldType.COLOR: ["is", "is_not", "in", "not_in"],
    FieldType.CURRENCY: [
        "is",
        "is_not",
        "less_than",
        "greater_than",
        "between",
        "not_between",
        "in",
        "not_in",
    ],
    FieldType.DATE: [
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
    FieldType.DATE_TIME: [
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
    FieldType.DURATION: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.ENTITY: [
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
    FieldType.FOOTAGE: ["is", "is_not"],
    FieldType.IMAGE: ["is", "is_not"],
    FieldType.LIST: ["is", "is_not", "in", "not_in"],
    FieldType.PASSWORD: [],
    FieldType.PERCENT: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.SERIALIZABLE: [],
    FieldType.STATUS_LIST: ["is", "is_not", "in", "not_in"],
    FieldType.SUMMARY: [],
    FieldType.TAG_LIST: [
        "is",
        "is_not",
        "name_contains",
        "name_not_contains",
        "name_id",
    ],
    FieldType.TIMECODE: [
        "is",
        "is_not",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ],
    FieldType.URL: [],
}
