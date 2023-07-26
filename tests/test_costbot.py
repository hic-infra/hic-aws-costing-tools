import json
import os.path
from datetime import datetime

import pytest

from hic_aws_costing_tools import aws_costs


def get_test_data(scenario, method, format="json"):
    file_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(file_dir, scenario, f"{method}.{format}")) as f:
        if format == "json":
            return json.load(f)
        return f.read()


def assert_2d_costs_equal(expected_costs, costs, decimal_places):
    assert len(expected_costs) == len(costs)
    for r, expected_r in enumerate(expected_costs):
        assert len(expected_r) == len(costs[r])
        for c, expected_c in enumerate(expected_r):
            if isinstance(expected_c, float):
                assert round(expected_c, decimal_places) == round(
                    costs[r][c], decimal_places
                )
            else:
                assert expected_c == costs[r][c]


@pytest.mark.parametrize("dimension", ["account", "accountname", "service", "Proj$"])
def test_get_group_by(mocker, dimension):
    if dimension == "service":
        scenario = "dummy-services"
        expected_group_by = {"Type": "DIMENSION", "Key": "SERVICE"}
        expected_values = {
            dv["Value"]
            for dv in get_test_data(scenario, "get_dimension_values-SERVICE")[
                "DimensionValues"
            ]
        }
        expected_value_map = {}
    elif dimension == "account":
        scenario = "dummy-services"
        expected_group_by = {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}
        expected_values = {"000000000001", "000000000002"}
        expected_value_map = {}
    elif dimension == "accountname":
        scenario = "dummy-services"
        expected_group_by = {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}
        expected_values = {"000000000001", "000000000002"}
        expected_value_map = {
            "000000000001": "researchers-1",
            "000000000002": "researchers-2",
        }
    else:
        scenario = "dummy-proj"
        expected_group_by = {"Type": "TAG", "Key": "Proj"}
        expected_values = set(
            f"Proj${t}" for t in get_test_data(scenario, "get_tags")["Tags"]
        )
        expected_value_map = {}

    time_period = {"Start": "2022-01-01", "End": "2022-01-02"}

    client_mock = mocker.Mock()

    def side_effect(*args, **kwargs):
        assert not args
        assert kwargs["TimePeriod"] == time_period
        return get_test_data(scenario, f"get_dimension_values-{kwargs['Dimension']}")

    if dimension == "Proj$":
        client_mock.get_tags.return_value = get_test_data(scenario, "get_tags")
    else:
        client_mock.get_dimension_values.side_effect = side_effect

    mocker.patch("boto3.client", return_value=client_mock)

    group_by, all_values, value_map = aws_costs._get_group_by(
        client_mock, time_period, dimension
    )
    assert group_by == expected_group_by
    assert all_values == expected_values
    assert value_map == expected_value_map


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_costs_for_regions(mocker, scenario):
    group1 = "accountname"
    if scenario == "dummy-services":
        group2 = "service"
        expected_services_or_tags = {
            dv["Value"]
            for dv in get_test_data(scenario, "get_dimension_values-SERVICE")[
                "DimensionValues"
            ]
        }
    else:
        group2 = "Proj$"
        expected_services_or_tags = set(
            f"Proj${t}" for t in get_test_data(scenario, "get_tags")["Tags"]
        )

    time_period = {"Start": "2022-01-01", "End": "2022-01-02"}

    client_mock = mocker.Mock()

    def side_effect(*args, **kwargs):
        assert not args
        assert kwargs["TimePeriod"] == time_period
        return get_test_data(scenario, f"get_dimension_values-{kwargs['Dimension']}")

    client_mock.get_dimension_values.side_effect = side_effect
    client_mock.get_cost_and_usage.return_value = get_test_data(
        scenario, "get_cost_and_usage"
    )
    if scenario == "dummy-proj":
        client_mock.get_tags.return_value = get_test_data(scenario, "get_tags")

    mocker.patch("boto3.client", return_value=client_mock)

    (
        results,
        all_values1,
        all_values2,
        value_map1,
        value_map2,
    ) = aws_costs.costs_for_regions(
        time_period=time_period,
        granularity="DAILY",
        regions=None,
        session=None,
        group1=group1,
        group2=group2,
        exclude_types=["Credit", "Refund"],
    )

    assert all_values1 == {"000000000001", "000000000002"}
    assert value_map1 == {
        "000000000001": "researchers-1",
        "000000000002": "researchers-2",
    }

    assert all_values2 == expected_services_or_tags
    assert value_map2 == {}

    assert results == get_test_data(scenario, "get_cost_and_usage")["ResultsByTime"]

    # TODO: split this method and check args for all API calls
    # assert client_mock.get_cost_and_usage.call_args.kwargs == args


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_costs_to_table(scenario):
    group1 = "AccountName"
    all_values1 = {"researchers-1", "researchers-2"}
    if scenario == "dummy-services":
        all_values2 = {
            dv["Value"]
            for dv in get_test_data(scenario, "get_dimension_values-SERVICE")[
                "DimensionValues"
            ]
        }
    else:
        all_values2 = set(
            f"Proj${t}" for t in get_test_data(scenario, "get_tags")["Tags"]
        )

    expected_output = get_test_data(scenario, "test-costs_to_table")
    expected_header = expected_output["header"]
    expected_costs = expected_output["costs"]

    results = get_test_data(scenario, "get_cost_and_usage")["ResultsByTime"]
    map_accounts = {
        "000000000001": "researchers-1",
        "000000000002": "researchers-2",
    }
    for r in results:
        for g in r["Groups"]:
            g["Keys"][0] = map_accounts[g["Keys"][0]]

    header, costs = aws_costs.costs_to_table(
        results=results,
        group1=group1,
        all_values1=all_values1,
        all_values2=all_values2,
        cost_type="UnblendedCost",
    )

    assert header == expected_header

    assert_2d_costs_equal(expected_costs, costs, 4)


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_costs_to_flat(scenario):
    accounts = {
        "000000000001": "researchers-1",
        "000000000002": "researchers-2",
    }
    if scenario == "dummy-services":
        services_or_tags = {
            dv["Value"]
            for dv in get_test_data(scenario, "get_dimension_values-SERVICE")[
                "DimensionValues"
            ]
        }
    else:
        services_or_tags = set(
            f"Proj${t}" for t in get_test_data(scenario, "get_tags")["Tags"]
        )
    services_or_tags = sorted(services_or_tags)

    expected_output = get_test_data(scenario, "test-costs_to_flat")
    expected_header = expected_output["header"]
    expected_costs = expected_output["costs"]

    results = get_test_data(scenario, "get_cost_and_usage")["ResultsByTime"]

    header, costs = aws_costs.costs_to_flat(
        results=results,
        accounts=accounts,
        cost_type="UnblendedCost",
    )

    assert header == expected_header

    assert_2d_costs_equal(expected_costs, costs, 4)


# @pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
# def test_sum_cost_table_over_accounts(scenario):
#     test_data = get_test_data(scenario, "test-costs_to_table")
#     header = test_data["header"]
#     costs = test_data["costs"]
#     expected_sum = test_data["account_sum"]

#     sum = aws_costs.sum_cost_table_over_accounts(header, costs)
#     assert_2d_costs_equal(expected_sum, sum, 4)


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_format_message_summarise(scenario):
    test_data = get_test_data(scenario, "test-costs_to_table")
    header = test_data["header"]
    costs = test_data["costs"]
    expected_output = get_test_data(scenario, "test-format_message_summarise", "md")

    m = aws_costs.format_message_summarise(header, "AccountName", costs)
    assert m == expected_output


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
@pytest.mark.parametrize("combine_accounts", [False])
def test_format_message_all(scenario, combine_accounts):
    if scenario == "dummy-services":
        group2 = "Service"
    else:
        group2 = "Proj$"
    test_data = get_test_data(scenario, "test-costs_to_table")
    header = test_data["header"]
    costs = test_data["costs"]
    expected_output = get_test_data(
        scenario, f"test-format_message_all-{combine_accounts}", "md"
    )

    m = aws_costs.format_message_all(
        header=header,
        costs=costs,
        group1="AccountName",
        group2=group2,
    )
    assert m == expected_output


@pytest.mark.parametrize(
    "startdate, days, enddate, expected_start, expected_end",
    [
        (datetime(2001, 2, 3).date(), 1, None, "2001-02-03", "2001-02-04"),
        ("2001-02-03", 1, None, "2001-02-03", "2001-02-04"),
        (datetime(2020, 11, 1).date(), 63, None, "2020-11-01", "2021-01-03"),
        (datetime(2020, 2, 10).date(), 1, "2021-02-03", "2020-02-10", "2021-02-03"),
    ],
)
def test_get_time_period(startdate, days, enddate, expected_start, expected_end):
    assert aws_costs.get_time_period(startdate, days, enddate) == {
        "Start": expected_start,
        "End": expected_end,
    }


def test_get_time_period_now(mocker):
    datetime_mock = mocker.Mock()
    datetime_mock.now.return_value = datetime(2012, 5, 3, 9, 56, 22)
    mocker.patch("hic_aws_costing_tools.aws_costs.datetime", datetime_mock)
    assert aws_costs.get_time_period(None, 1, None) == {
        "Start": "2012-05-02",
        "End": "2012-05-03",
    }
