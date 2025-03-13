"""A pure python implementation of a shotgrid compatible query engine.

This will be slower than sql_alchemy, but useful if you can't install that package!
"""

from typing import Any

from fakegrid.schema import Schema

from .base import BackendQuery, BaseBackend

EntityName = str
Id = int
EntityData = dict[str, Any]
DataStore = dict[EntityName, dict[Id, EntityData]]


class PurePythonBackend(BaseBackend):
    """An in-memory pure python implementation of a shotgun query backend."""

    def __init__(self, schema: Schema):
        super().__init__(schema)
        self._data: DataStore = {}

    def find(self, query: BackendQuery) -> list[dict[str, Any]]:
        entity_data = self._data[query.entity.api_name]
        results = list(entity_data.values())
        return results
