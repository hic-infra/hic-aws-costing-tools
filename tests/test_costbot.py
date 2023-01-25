from datetime import datetime
import json
from msteams_costbot import app
import os.path
import pytest


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


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_costs_for_regions(mocker, scenario):
    if scenario == "dummy-services":
        expected_services_or_tags = {
            dv["Value"]
            for dv in get_test_data(scenario, "get_dimension_values-SERVICE")[
                "DimensionValues"
            ]
        }
        service_or_tag = None
    else:
        expected_services_or_tags = set(
            f"Proj${t}" for t in get_test_data(scenario, "get_tags")["Tags"]
        )
        service_or_tag = "Proj"

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

    results, all_linked_accounts, all_services_or_tags = app.costs_for_regions(
        time_period=time_period,
        granularity="DAILY",
        regions=None,
        session=None,
        service_or_tag=service_or_tag,
        exclude_types=["Credit", "Refund"],
    )

    assert all_linked_accounts == {
        "000000000001": "researchers-1",
        "000000000002": "researchers-2",
    }
    assert all_services_or_tags == expected_services_or_tags
    assert results == get_test_data(scenario, "get_cost_and_usage")["ResultsByTime"]

    # TODO: split this method and check args for all API calls
    # assert client_mock.get_cost_and_usage.call_args.kwargs == args


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_costs_to_table(scenario):
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

    expected_output = get_test_data(scenario, "test-costs_to_table")
    expected_header = expected_output["header"]
    expected_costs = expected_output["costs"]

    results = get_test_data(scenario, "get_cost_and_usage")["ResultsByTime"]

    header, costs = app.costs_to_table(
        results=results,
        accounts=accounts,
        services_or_tags=services_or_tags,
        cost_type="UnblendedCost",
    )

    assert header == expected_header

    assert_2d_costs_equal(expected_costs, costs, 4)


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_sum_cost_table_over_accounts(scenario):
    test_data = get_test_data(scenario, "test-costs_to_table")
    header = test_data["header"]
    costs = test_data["costs"]
    expected_sum = test_data["account_sum"]

    sum = app.sum_cost_table_over_accounts(header, costs)
    assert_2d_costs_equal(expected_sum, sum, 4)


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
def test_format_message_summarise(scenario):
    test_data = get_test_data(scenario, "test-costs_to_table")
    header = test_data["header"]
    costs = test_data["costs"]
    expected_output = get_test_data(scenario, f"test-format_message_summarise", "md")

    m = app.format_message_summarise(header, costs)
    assert m == expected_output


@pytest.mark.parametrize("scenario", ["dummy-services", "dummy-proj"])
@pytest.mark.parametrize("combine_accounts", [True, False])
def test_format_message_all(scenario, combine_accounts):
    test_data = get_test_data(scenario, "test-costs_to_table")
    header = test_data["header"]
    costs = test_data["costs"]
    expected_output = get_test_data(
        scenario, f"test-format_message_all-{combine_accounts}", "md"
    )

    m = app.format_message_all(
        header=header,
        costs=costs,
        service_or_tag="Proj",
        combine_accounts=combine_accounts,
    )
    assert m == expected_output


# def send_teams(*, message, title, webhook):
# def create_costs_message(


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
    assert app.get_time_period(startdate, days, enddate) == {
        "Start": expected_start,
        "End": expected_end,
    }


def test_get_time_period_now(mocker):
    datetime_mock = mocker.Mock()
    datetime_mock.now.return_value = datetime(2012, 5, 3, 9, 56, 22)
    mocker.patch("msteams_costbot.app.datetime", datetime_mock)
    assert app.get_time_period(None, 1, None) == {
        "Start": "2012-05-02",
        "End": "2012-05-03",
    }


# def lambda_handler(event, context):
# def main(args):
