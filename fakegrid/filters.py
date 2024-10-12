"""Filters and operators for querying data in shotgrid."""

from enum import Enum

from .schema import FieldType


class FilterOperator(Enum):
    """All valid operators for filtering."""

    IS = "is"
    IS_NOT = "is_not"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"
    IN = "in"
    NOT_IN = "not_in"
    IN_LAST = "in_last"
    NOT_IN_LAST = "not_in_last"
    IN_NEXT = "in_next"
    NOT_IN_NEXT = "not_in_next"
    IN_CALENDAR_DAY = "in_calendar_day"
    IN_CALENDAR_WEEK = "in_calendar_week"
    IN_CALENDAR_MONTH = "in_calendar_month"
    IN_CALENDAR_YEAR = "in_calendar_year"
    TYPE_IS = "type_is"
    TYPE_IS_NOT = "type_is_not"
    NAME_CONTAINS = "name_contains"
    NAME_NOT_CONTAINS = "name_not_contains"
    NAME_IS = "name_is"
    NAME_STARTS_WITH = "name_starts_with"
    NAME_ENDS_WITH = "name_ends_with"


ALLOWED_OPERATIONS_BY_FIELD_TYPE = {
    FieldType.TEXT: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.CONTAINS,
        FilterOperator.NOT_CONTAINS,
        FilterOperator.STARTS_WITH,
        FilterOperator.ENDS_WITH,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.FLOAT: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.MULTI_ENTITY: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.TYPE_IS,
        FilterOperator.TYPE_IS_NOT,
        FilterOperator.NAME_CONTAINS,
        FilterOperator.NAME_NOT_CONTAINS,
        FilterOperator.NAME_IS,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.NUMBER: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.LESS_THAN,
        FilterOperator.GREATER_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.NOT_BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.ADDRESSING: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.CONTAINS,
        FilterOperator.NOT_CONTAINS,
        FilterOperator.IN,
        FilterOperator.TYPE_IS,
        FilterOperator.TYPE_IS_NOT,
        FilterOperator.NAME_CONTAINS,
        FilterOperator.NAME_NOT_CONTAINS,
        FilterOperator.NAME_STARTS_WITH,
        FilterOperator.NAME_ENDS_WITH,
    ],
    FieldType.CHECKBOX: [FilterOperator.IS, FilterOperator.IS_NOT],
    FieldType.COLOR: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.CURRENCY: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.LESS_THAN,
        FilterOperator.GREATER_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.NOT_BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.DATE: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.IN_LAST,
        FilterOperator.NOT_IN_LAST,
        FilterOperator.IN_NEXT,
        FilterOperator.NOT_IN_NEXT,
        FilterOperator.IN_CALENDAR_DAY,
        FilterOperator.IN_CALENDAR_WEEK,
        FilterOperator.IN_CALENDAR_MONTH,
        FilterOperator.IN_CALENDAR_YEAR,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.DATE_TIME: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.IN_LAST,
        FilterOperator.NOT_IN_LAST,
        FilterOperator.IN_NEXT,
        FilterOperator.NOT_IN_NEXT,
        FilterOperator.IN_CALENDAR_DAY,
        FilterOperator.IN_CALENDAR_WEEK,
        FilterOperator.IN_CALENDAR_MONTH,
        FilterOperator.IN_CALENDAR_YEAR,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.DURATION: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.ENTITY: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.TYPE_IS,
        FilterOperator.TYPE_IS_NOT,
        FilterOperator.NAME_CONTAINS,
        FilterOperator.NAME_NOT_CONTAINS,
        FilterOperator.NAME_IS,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.FOOTAGE: [FilterOperator.IS, FilterOperator.IS_NOT],
    FieldType.IMAGE: [FilterOperator.IS, FilterOperator.IS_NOT],
    FieldType.LIST: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.PASSWORD: [],
    FieldType.PERCENT: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.SERIALIZABLE: [],
    FieldType.STATUS_LIST: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.SUMMARY: [],
    FieldType.TAG_LIST: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.NAME_CONTAINS,
        FilterOperator.NAME_NOT_CONTAINS,
        FilterOperator.NAME_IS,
    ],
    FieldType.TIMECODE: [
        FilterOperator.IS,
        FilterOperator.IS_NOT,
        FilterOperator.GREATER_THAN,
        FilterOperator.LESS_THAN,
        FilterOperator.BETWEEN,
        FilterOperator.IN,
        FilterOperator.NOT_IN,
    ],
    FieldType.URL: [],
}
