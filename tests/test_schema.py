import json
from pathlib import Path

from fakegrid.schema import FieldType, Schema

DEMO_SITE_DATA = Path(__file__).parent / "test_data" / "demo_site"

DEMO_SCHEMA_READ = DEMO_SITE_DATA / "schema_read.json"
DEMO_SCHEMA_ENTITY_READ = DEMO_SITE_DATA / "schema_entity_read.json"
DEMO_JS_SCHEMA = DEMO_SITE_DATA / "reload_schema.json"


def test_read_demo_schema() -> None:
    schema_read = json.loads(DEMO_SCHEMA_READ.read_text())
    schema_entity_read = json.loads(DEMO_SCHEMA_ENTITY_READ.read_text())
    js_schema = json.loads(DEMO_JS_SCHEMA.read_text())

    schema = Schema.from_json(schema_read, schema_entity_read, js_schema)

    # Double check that we have the right number of entities
    non_connection_entities = [e for e in schema.entities if not e.api_name.endswith("Connection")]
    assert len(non_connection_entities) == 69

    # And make sure we have no entity or multi-entity fields that
    # aren't linked to something
    for entity in schema.entities:
        for field in entity.fields:
            if field.field_type in (FieldType.ENTITY, FieldType.MULTI_ENTITY):
                if not field.link:
                    if field.field_type == FieldType.ENTITY:
                        continue
                    print(f"{entity.api_name} -> {field.api_name}")


if __name__ == "__main__":
    test_read_demo_schema()
