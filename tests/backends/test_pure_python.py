from pathlib import Path

import pytest

from fakegrid.backends.base import BackendComplexFilter, BackendFilter, BackendQuery, FieldPath
from fakegrid.backends.pure_python import PurePythonBackend
from fakegrid.filters import ComplexFilterOperator, FilterOperator
from fakegrid.schema import Schema
from fakegrid.schema_parse import build_fakegrid_schema

ROOT = Path(__file__).parent.parent.parent
SCHEMA_PATH = ROOT / "tests" / "test_data" / "demo_site" / "reload_schema.json"


@pytest.fixture(scope="module")
def schema() -> Schema:
    return build_fakegrid_schema(SCHEMA_PATH)


def test_pure_python_backend(schema: Schema) -> None:
    backend = PurePythonBackend(schema)
    backend._data = {
        "Version": {
            1: {"id": 1, "code": "v1", "description": "first version"},
            2: {"id": 2, "code": "v2", "description": "second version"},
        }
    }

    version_entity = schema.get_entity("Version")
    assert version_entity is not None

    results = backend.find(
        BackendQuery(
            entity=version_entity,
            filters=BackendComplexFilter(
                operator=ComplexFilterOperator.ALL,
                filters=[
                    BackendFilter(
                        field=FieldPath.from_string(version_entity, "id"),
                        operator=FilterOperator.IS,
                        value=1,
                    ),
                ],
            ),
            return_fields=[],
            order=[],
            limit=None,
            page=None,
            retired_only=False,
            include_archived_projects=False,
        )
    )

    assert len(results) == 1
