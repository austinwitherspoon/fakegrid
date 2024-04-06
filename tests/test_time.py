import pytest
import datetime
from fakegrid import Fakegrid
import shotgun_api3.shotgun

def test_time_queries(clean_sg: Fakegrid):
	new_project_1 = clean_sg.create("Project", {"name": "Test Project 1", "code": "TP1", "created_at": datetime.datetime(2020, 1, 1, 0, 0, 0)})
	new_project_2 = clean_sg.create("Project", {"name": "Test Project 2", "code": "TP2", "created_at": datetime.datetime(2021, 1, 1, 0, 0, 0)})
	new_project_3 = clean_sg.create("Project", {"name": "Test Project 3", "code": "TP3", "created_at": datetime.datetime(2021, 2, 1, 0, 0, 0)})
	new_project_4 = clean_sg.create("Project", {"name": "Test Project 4", "code": "TP4", "created_at": datetime.datetime(2021, 3, 1, 0, 0, 0)})
	new_project_5 = clean_sg.create("Project", {"name": "Test Project 5", "code": "TP5", "created_at": datetime.datetime(2021, 3, 1, 9, 0, 0)})
	new_project_6 = clean_sg.create("Project", {"name": "Test Project 6", "code": "TP6", "created_at": datetime.datetime(2021, 3, 1, 9, 30, 0)})

	clean_sg._now_function = lambda *_: datetime.datetime(2021, 3, 1, 9, 0, 0, tzinfo=shotgun_api3.shotgun.SgTimezone.LocalTimezone())

	# is query
	assert clean_sg.find("Project", [["created_at", "is", datetime.datetime(2021, 2, 1, 0, 0, 0)]])[0]["id"] == new_project_3["id"]


	# is_not query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "is_not", datetime.datetime(2021, 2, 1, 0, 0, 0)]])]
	assert new_project_3["id"] not in found_ids
	assert len(found_ids) == 5

	# greater_than query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "greater_than", datetime.datetime(2021, 2, 1, 0, 0, 0)]])]
	assert found_ids == [4,5,6]

	# less_than query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "less_than", datetime.datetime(2021, 2, 1, 0, 0, 0)]])]
	assert found_ids == [1,2]

	# between query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "between", [datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 3, 1, 0, 0, 0)]]])]
	assert found_ids == [2,3,4]

	# not_between query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "not_between", [datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 3, 1, 0, 0, 0)]]])]
	assert found_ids == [1,5,6]

	# in query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in", [datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 3, 1, 0, 0, 0)]]])]
	assert found_ids == [2,4]

	# not_in query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "not_in", [datetime.datetime(2021, 1, 1, 0, 0, 0), datetime.datetime(2021, 3, 1, 0, 0, 0)]]])]
	assert found_ids == [1,3,5,6]

	# in_last query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_last", 1, "HOUR"]])]
	assert found_ids == [5]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_last", 1, "DAY"]])]
	assert found_ids == [4,5]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_last", 1, "WEEK"]])]
	assert found_ids == [4,5]
	
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_last", 1, "MONTH"]])]
	assert found_ids == [3,4,5]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_last", 1, "YEAR"]])]
	assert found_ids == [2,3,4,5]

	# in_next query
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_next", 1, "HOUR"]])]
	assert found_ids == [5, 6]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "not_in_next", 1, "DAY"]])]
	assert found_ids == [1,2,3,4]

	# in_calendar_day
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_day", 0]])]
	assert found_ids == [4,5,6]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_day", -28]])]
	assert found_ids == [3]

	# in_calendar_week
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_week", 0]])]
	assert found_ids == [4,5,6]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_week", -4]])]
	assert found_ids == [3]

	# in_calendar_month
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_month", 0]])]
	assert found_ids == [4,5,6]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_month", -1]])]
	assert found_ids == [3]

	# in_calendar_year
	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_year", 0]])]
	assert found_ids == [2,3,4,5,6]

	found_ids = [i["id"] for i in clean_sg.find("Project", [["created_at", "in_calendar_year", -1]])]
	assert found_ids == [1]
