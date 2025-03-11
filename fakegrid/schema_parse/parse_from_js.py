from fakegrid.schema import Schema

from .js_schema import JsSchema


def build_fakegrid_schema(raw_schema: JsSchema) -> Schema:
    """Use the raw schema data to build a fakegrid schema."""
    schema = Schema(entities=[])

    return schema


# TODO: add ranked field connection resolvers
