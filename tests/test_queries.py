import pickle
import pytest
import shotgun_api3
from datetime import datetime

from sqlalchemy import create_engine

import fakegrid

# artists taken from demo site
ARTIST_1 = {"type": "HumanUser", "id": 19}
ARTIST_2 = {"type": "HumanUser", "id": 18}
SHOTGRID_SUPPORT = {"type": "HumanUser", "id": 24}
ZEBRA_VERSION = {"id": 6944, "name": "alias_model_Zebra", "type": "Version"}

LOCAL_TIMEZONE = shotgun_api3.sg_timezone.LocalTimezone()

# Queries to test the reading and filtering of different data types
TEST_QUERIES = [
    # Addressing
    ("Note", [], ["addressings_to", "addressings_cc"]),
    (
        "Note",
        [["addressings_to", "is", ARTIST_1]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "is_not", ARTIST_1]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "contains", ARTIST_1]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "not_contains", ARTIST_1]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "in", [ARTIST_1, ARTIST_2]]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "is", [ARTIST_1, ARTIST_2]]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "in", ARTIST_1]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "type_is", "HumanUser"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "type_is_not", "HumanUser"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "name_contains", "a"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "name_not_contains", "a"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "name_starts_with", "a"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [["addressings_to", "name_ends_with", "a"]],
        ["addressings_to", "addressings_cc"],
    ),
    (
        "Note",
        [
            ["addressings_to", "greater_than", ARTIST_1],
            ["addressings_cc", "is", ARTIST_2],
        ],
        ["addressings_to", "addressings_cc"],
    ),
    # Checkbox
    ("Note", [], ["client_note"]),
    ("Note", [["client_note", "is", True]], ["client_note"]),
    ("Note", [["client_note", "is_not", True]], ["client_note"]),
    ("Note", [["client_note", "greater_than", 0]], ["client_note"]),
    # Color
    ("Step", [], ["color"]),
    ("Step", [["color", "is", "50,149,253"]], ["color"]),
    ("Step", [["color", "is_not", "50,149,253"]], ["color"]),
    (
        "Step",
        [["color", "in", "50,149,253"]],  # unintuitive but valid I guess!
        ["color"],
    ),
    ("Step", [["color", "not_in", "50,149,253"]], ["color"]),
    ("Step", [["color", "in", ["50,149,253"]]], ["color"]),
    ("Step", [["color", "not_in", ["50,149,253"]]], ["color"]),
    # Currency - no currency field on demo site?
    # Date
    ("Task", [], ["due_date"]),
    ("Task", [["due_date", "is", "2016-01-18"]], ["due_date"]),
    ("Task", [["due_date", "is_not", "2016-01-18"]], ["due_date"]),
    ("Task", [["due_date", "greater_than", "2016-01-18"]], ["due_date"]),
    ("Task", [["due_date", "less_than", "2016-01-18"]], ["due_date"]),
    ("Task", [["due_date", "in_last", 1, "DAY"]], ["due_date"]),
    ("Task", [["due_date", "not_in_last", 1, "DAY"]], ["due_date"]),
    ("Task", [["due_date", "in_last", 10, "SECOND"]], ["due_date"]),  # bad
    ("Task", [["due_date", "in_last", 10, "HOUR"]], ["due_date"]),
    ("Task", [["due_date", "in_last", 10, "MONTH"]], ["due_date"]),
    ("Task", [["due_date", "in_last", 10, "YEAR"]], ["due_date"]),
    ("Task", [["due_date", "in_next", 1, "DAY"]], ["due_date"]),
    ("Task", [["due_date", "not_in_next", 1, "DAY"]], ["due_date"]),
    ("Task", [["due_date", "in_calendar_day", 0]], ["due_date"]),
    ("Task", [["due_date", "in_calendar_week", 0]], ["due_date"]),
    ("Task", [["due_date", "in_calendar_month", 0]], ["due_date"]),
    ("Task", [["due_date", "in_calendar_year", 0]], ["due_date"]),
    ("Task", [["due_date", "between", ["2016-01-18", "2016-02-18"]]], ["due_date"]),
    ("Task", [["due_date", "in", ["2016-01-18"]]], ["due_date"]),
    ("Task", [["due_date", "not_in", ["2016-01-18"]]], ["due_date"]),
    # Date/Time
    ("Version", [], ["created_at"]),
    (
        "Version",
        [["created_at", "is", datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE)]],
        ["created_at"],
    ),
    (
        "Version",
        [["created_at", "is_not", datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE)]],
        ["created_at"],
    ),
    (
        "Version",
        [
            [
                "created_at",
                "greater_than",
                datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE),
            ]
        ],
        ["created_at"],
    ),
    (
        "Version",
        [
            [
                "created_at",
                "less_than",
                datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE),
            ]
        ],
        ["created_at"],
    ),
    (
        "Version",
        [
            [
                "created_at",
                "between",
                [
                    datetime(2015, 12, 1, 8, 50, tzinfo=LOCAL_TIMEZONE),
                    datetime(2015, 12, 1, 8, 55, tzinfo=LOCAL_TIMEZONE),
                ],
            ]
        ],
        ["created_at"],
    ),
    (
        "Version",
        [["created_at", "in", [datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE)]]],
        ["created_at"],
    ),
    (
        "Version",
        [
            [
                "created_at",
                "not_in",
                [datetime(2015, 12, 1, 8, 51, tzinfo=LOCAL_TIMEZONE)],
            ]
        ],
        ["created_at"],
    ),
    # Duration
    ("Task", [], ["duration"]),
    ("Task", [["duration", "is", 3000]], ["duration"]),
    ("Task", [["duration", "is_not", 3000]], ["duration"]),
    ("Task", [["duration", "greater_than", 3000]], ["duration"]),
    ("Task", [["duration", "less_than", 3000]], ["duration"]),
    ("Task", [["duration", "between", [3000, 3600]]], ["duration"]),
    ("Task", [["duration", "in", [3000, 3600]]], ["duration"]),
    ("Task", [["duration", "not_in", [3000, 3600]]], ["duration"]),
    # Entity
    ("Version", [], ["created_by"]),
    ("Version", [["created_by", "is", ARTIST_1]], ["created_by"]),
    ("Version", [["created_by", "is_not", ARTIST_1]], ["created_by"]),
    ("Version", [["entity", "type_is", "Shot"]], ["entity"]),
    ("Version", [["entity", "type_is_not", "Shot"]], ["entity"]),
    ("Version", [["created_by", "name_contains", "1"]], ["created_by"]),
    ("Version", [["created_by", "name_not_contains", "1"]], ["created_by"]),
    ("Version", [["created_by", "name_is", "Artist 1"]], ["created_by"]),
    ("Version", [["created_by", "in", [ARTIST_1, ARTIST_2]]], ["created_by"]),
    ("Version", [["created_by", "not_in", [ARTIST_1, ARTIST_2]]], ["created_by"]),
    # Float
    ("Version", [], ["sg_uploaded_movie_frame_rate"]),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "is", 24.0]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "is_not", 24.0]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "greater_than", 23.0]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "less_than", 24.1]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "between", [24.0, 30.0]]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "in", [24.0, 30.0]]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    (
        "Version",
        [["sg_uploaded_movie_frame_rate", "not_in", [24.0, 30.0]]],
        ["sg_uploaded_movie_frame_rate"],
    ),
    # footage - no footage field on demo site?
    # Image - Don't test results because our demo database
    # may copy images to local dir and change result
    ("Asset", [["image", "is", None]], ["image"]),
    # List
    ("Asset", [], ["sg_asset_type"]),
    ("Asset", [["sg_asset_type", "is", "Character"]], ["sg_asset_type"]),
    ("Asset", [["sg_asset_type", "is_not", "Character"]], ["sg_asset_type"]),
    ("Asset", [["sg_asset_type", "in", ["Character"]]], ["sg_asset_type"]),
    ("Asset", [["sg_asset_type", "not_in", ["Character"]]], ["sg_asset_type"]),
    # Multi-Entity - lets test both natural multi-entity fields
    # and reverse fields of single entity links
    ("Project", [], ["users"]),
    ("Project", [["users", "is", SHOTGRID_SUPPORT]], ["users"]),
    ("Project", [["users", "is_not", SHOTGRID_SUPPORT]], ["users"]),
    ("Project", [["users", "type_is", "HumanUser"]], ["users"]),
    ("Project", [["users", "type_is_not", "HumanUser"]], ["users"]),
    ("Project", [["users", "name_contains", "Support"]], ["users"]),
    ("Project", [["users", "name_not_contains", "Support"]], ["users"]),
    ("Project", [["users", "name_is", "Shotgrid Support"]], ["users"]),
    ("Project", [["users", "in", [SHOTGRID_SUPPORT, ARTIST_2]]], ["users"]),
    ("Project", [["users", "not_in", [SHOTGRID_SUPPORT, ARTIST_2]]], ["users"]),
    (
        "Project",
        [["users", "is", ARTIST_1]],
        ["users"],
    ),  # won't work - account disabled
    ("Asset", [], ["sg_versions"]),
    ("Asset", [["sg_versions", "is", ZEBRA_VERSION]], ["sg_versions"]),
    ("Asset", [["sg_versions", "is_not", ZEBRA_VERSION]], ["sg_versions"]),
    ("Asset", [["sg_versions", "type_is", "Version"]], ["sg_versions"]),
    ("Asset", [["sg_versions", "type_is_not", "Version"]], ["sg_versions"]),
    ("Asset", [["sg_versions", "name_contains", "Zebra"]], ["sg_versions"]),
    ("Asset", [["sg_versions", "name_not_contains", "Zebra"]], ["sg_versions"]),
    ("Asset", [["sg_versions", "name_is", "alias_model_Zebra"]], ["sg_versions"]),
    ("Asset", [["sg_versions", "in", [ZEBRA_VERSION]]], ["sg_versions"]),
    ("Asset", [["sg_versions", "not_in", [ZEBRA_VERSION]]], ["sg_versions"]),
    # number
    ("Version", [], ["id"]),
    ("Version", [["id", "is", 6947]], ["id"]),
    ("Version", [["id", "is_not", 6947]], ["id"]),
    ("Version", [["id", "less_than", 6947]], ["id"]),
    ("Version", [["id", "greater_than", 6947]], ["id"]),
    ("Version", [["id", "between", [6947, 6949]]], ["id"]),
    ("Version", [["id", "in", [6947, 6949]]], ["id"]),
    ("Version", [["id", "not_in", [6947, 6949]]], ["id"]),
    # password
    ("HumanUser", [], ["password_proxy"]),
    ("HumanUser", [["password_proxy", "is", "test"]], ["password_proxy"]),
    # Percent
    ("HumanUser", [], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "is", 100]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "is_not", 100]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "less_than", 100]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "greater_than", 100]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "between", [5, 100]]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "in", [99, 100]]], ["percent_capacity"]),
    ("HumanUser", [["percent_capacity", "not_in", [1, 2]]], ["percent_capacity"]),
    # Serializable
    ("EventLogEntry", [], ["meta"]),
    # Status List
    ("Task", [], ["sg_status_list"]),
    ("Task", [["sg_status_list", "is", "ip"]], ["sg_status_list"]),
    ("Task", [["sg_status_list", "is_not", "ip"]], ["sg_status_list"]),
    ("Task", [["sg_status_list", "in", ["ip"]]], ["sg_status_list"]),
    ("Task", [["sg_status_list", "not_in", ["ip"]]], ["sg_status_list"]),
    # Tag List - no tags in demo site
    # Text
    ("Version", [], ["code"]),
    ("Version", [["code", "is", "bunny_080_0200_layout_v000"]], ["code"]),
    ("Version", [["code", "is_not", "bunny_080_0200_layout_v000"]], ["code"]),
    ("Version", [["code", "contains", "bunny_080_0200"]], ["code"]),
    ("Version", [["code", "not_contains", "bunny_080_0200"]], ["code"]),
    ("Version", [["code", "starts_with", "bunny"]], ["code"]),
    ("Version", [["code", "ends_with", "v000"]], ["code"]),
    (
        "Version",
        [["code", "in", ["bunny_080_0200_layout_v000", "bunny_080_0200_layout_v001"]]],
        ["code"],
    ),
    (
        "Version",
        [
            [
                "code",
                "not_in",
                ["bunny_080_0200_layout_v000", "bunny_080_0200_layout_v001"],
            ]
        ],
        ["code"],
    ),
    # Timecode - no timecode field on demo site?
    # url
    ("Version", [], ["sg_uploaded_movie"]),
    # other interesting scenarios
    # projects only show active users- so all demo site disabled users won't appear here
    ("Project", [], ["users"]),
    # and querying won't work!
    ("Project", [["users", "is", ARTIST_1]], ["users"]),
    # dotted syntax
    (
        "Version",
        [["entity.Asset.code", "is", "Assembly"]],
        ["code", "entity", "entity.Shot.addressings_cc"],
    ),
    (
        "Shot",
        [["sg_versions.Version.project.Project.id", "is", 70]],
        ["sg_versions", "code"],
    ),
    (
        "Shot",
        [["sg_versions.Version.project.Project.id", "is", None]],
        ["sg_versions", "code"],
    ),
    (
        "Shot",
        [["sg_versions.Version.entity.Shot.id", "is", None]],
        ["sg_versions", "code"],
    ),
    (
        "Shot",
        [["sg_versions.Version.entity", "is", None]],
        ["sg_versions", "code"],
    ),
    (
        "Shot",
        [["sg_versions.Version.entity.Shot.id", "is", 1021]],
        ["sg_versions", "code"],
    ),
    (
        "Shot",
        [["sg_versions.Version.open_notes.Note.project.Project.id", "is", 70]],
        ["sg_versions", "code"],
    ),
    # stuff that doesn't exist
    ("NotAnEntity", [], ["code"]),
    ("Version", [], ["not_a_field"]),
    ("Version", [["not_a_field", "is", "test"]], ["not_a_field"]),
    ("Version", [["code", "not_an_operator", "test"]], ["code"]),
    # complex queries
    (
        "Version",
        [
            {
                "filter_operator": "any",
                "filters": [
                    ["code", "is", "bunny_080_0200_layout_v000"],
                    ["code", "is", "bunny_080_0200_layout_v001"],
                ],
            }
        ],
        ["code"],
    ),
    (
        "Version",
        [
            {
                "filter_operator": "all",
                "filters": [
                    ["code", "contains", "bunny"],
                    {
                        "filter_operator": "any",
                        "filters": [
                            ["code", "contains", "0200"],
                            ["code", "is", "layout"],
                        ],
                    },
                ],
            }
        ],
        ["code"],
    ),
]

from pathlib import Path
from fakegrid.fakegrid import Fakegrid

expected_results = pickle.load(
    open(Path(__file__).parent.parent / "test/real_test_results.pickle", "rb")
)


@pytest.fixture(scope="module")
def sg():
    import sqlite3

    source = sqlite3.connect("demo_site.sqlite")
    engine = create_engine("sqlite:///")
    source.backup(engine.raw_connection().driver_connection)
    schema = fakegrid.ShotgridSchema.from_file(
        Path(__file__).parent.parent / "schema.pickle"
    )
    return Fakegrid.from_schema(schema, engine)


@pytest.mark.parametrize("entity_type, filters, fields", TEST_QUERIES)
def test_queries(entity_type, filters, fields, sg):
    key = "{} {} {}".format(entity_type, filters, fields)
    expected_result = expected_results[key]
    try:
        our_result = sg.find(entity_type, filters, fields)
    except Exception as e:
        our_result = e
    if isinstance(expected_result, Exception):
        assert type(our_result) == type(expected_result)
    else:
        assert our_result == expected_result
