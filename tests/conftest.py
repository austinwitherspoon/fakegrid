from pathlib import Path
import sys
from sqlalchemy import create_engine
import pytest
from fakegrid import fakegrid

sys.path.append(str(Path(__file__).parent.parent))

@pytest.fixture(scope="module")
def sg():
    import sqlite3

    source = sqlite3.connect("demo_site.sqlite")
    engine = create_engine("sqlite:///")
    source.backup(engine.raw_connection().driver_connection)  # type:ignore
    schema = fakegrid.ShotgridSchema.from_file(
        Path(__file__).parent.parent / "schema.pickle"
    )
    return fakegrid.Fakegrid.from_schema(schema, engine)

@pytest.fixture(scope="function")
def clean_sg():
    schema = fakegrid.ShotgridSchema.from_file(
        Path(__file__).parent.parent / "schema.pickle"
    )
    return fakegrid.Fakegrid.from_schema(schema)

@pytest.fixture(scope="module")
def debug_sg():
    import sqlite3

    engine = create_engine("sqlite:///debug.sqlite")
    schema = fakegrid.ShotgridSchema.from_file(
        Path(__file__).parent.parent / "schema.pickle"
    )
    return fakegrid.Fakegrid.from_schema(schema, engine)