import csv
from datetime import datetime, timedelta
from io import StringIO

import boto3

DEFAULT_COST_TYPE = "UnblendedCost"
DEFAULT_GRANULARITY = "MONTHLY"
# Previously we excluded these types by default. Now we just include Usage instead.
DEFAULT_EXCLUDE_RECORD_TYPES = [
    # "Credit",
    # "Refund",
    # "Tax",
    # # These two aren't documented on
    # # https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/manage-cost-categories.html#cost-categories-terms
    # # but were confirmed in AWS Support ticket 171570162800825
    # "Enterprise Discount Program Discount",
    # "Solution Provider Program Discount",
]
# https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/manage-cost-categories.html#cost-categories-terms
DEFAULT_INCLUDE_RECORD_TYPES = ["Usage"]
EXPECTED_UNIT = "USD"


def _get_group_by(ce, time_period, dimension):
    """
    Get the group by query for the given dimension
    :return (group by query, all values for the dimension, optional mapping of values to display names)
    """
    value_map = {}
    if dimension[-1] == "$":
        dim = dimension[:-1]
        group_by = {"Type": "TAG", "Key": dim}
        r = ce.get_tags(TimePeriod=time_period, TagKey=dim)
        all_values = set(f"{dim}${t}" for t in r["Tags"])
        return group_by, all_values, value_map

    dim = dimension.upper()
    if dim in ("ACCOUNT", "ACCOUNTNAME"):
        group_by = {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}
        r = ce.get_dimension_values(TimePeriod=time_period, Dimension="LINKED_ACCOUNT")
        all_values = set(dv["Value"] for dv in r["DimensionValues"])
        if dim == "ACCOUNTNAME":
            value_map = dict(
                (dv["Value"], dv["Attributes"]["description"])
                for dv in r["DimensionValues"]
            )
        return group_by, all_values, value_map

    if dim == "SERVICE":
        group_by = {"Type": "DIMENSION", "Key": dim}
        r = ce.get_dimension_values(TimePeriod=time_period, Dimension=dim)
        all_values = set(dv["Value"] for dv in r["DimensionValues"])
        return group_by, all_values, value_map

    raise ValueError(f"Invalid dimension: {dimension}")


def _get_filter(regions, exclude_types, include_types):
    filter_count = 0
    region_filter = None
    exclude_filter = None
    include_filter = None
    filter = None

    if regions:
        filter_count += 1
        region_filter = dict(
            Dimensions={
                "Key": "REGION",
                "Values": regions,
            }
        )
    if exclude_types:
        filter_count += 1
        exclude_filter = dict(
            Not=dict(
                Dimensions={
                    "Key": "RECORD_TYPE",
                    "Values": exclude_types,
                }
            )
        )
    if include_types:
        filter_count += 1
        include_filter = dict(
            Dimensions={
                "Key": "RECORD_TYPE",
                "Values": include_types,
            }
        )

    if filter_count > 1:
        filter = dict(And=[])
    for f in [region_filter, exclude_filter, include_filter]:
        if f:
            if filter:
                filter["And"].append(f)
            else:
                filter = f

    return filter


def costs_for_regions(
    *,
    time_period,
    granularity,
    regions,
    session,
    group1,
    group2,
    exclude_types,
    include_types,
):
    if session:
        ce = session.client("ce")
    else:
        ce = boto3.client("ce")

    group_by1, all_values1, value_map1 = _get_group_by(ce, time_period, group1)
    group_by2, all_values2, value_map2 = _get_group_by(ce, time_period, group2)

    r = None
    results = []
    kwargs = dict(
        Granularity=granularity,
        GroupBy=[group_by1, group_by2],
        Metrics=["UnblendedCost"],
        TimePeriod=time_period,
    )

    filter = _get_filter(regions, exclude_types, include_types)
    if filter:
        kwargs["Filter"] = filter

    while not r or "NextPageToken" in r:
        # print(f"get_cost_and_usage({kwargs})")
        r = ce.get_cost_and_usage(**kwargs)
        results.extend(r["ResultsByTime"])

    return results, all_values1, all_values2, value_map1, value_map2


def costs_to_table(*, results, group1, all_values1, all_values2, cost_type):
    # results will have one group per day/month
    all_values2_sorted = sorted(all_values2)

    header = [group1] + all_values2_sorted + ["TOTAL"]
    costs = [[0] * len(header) for _ in range(len(all_values1))]

    for result in results:
        g1_g2_map = {}
        for g in result["Groups"]:
            # [group1, group2]
            if g["Metrics"][cost_type]["Unit"] != EXPECTED_UNIT:
                raise RuntimeError(
                    f"Unexpected unit: {g['Metrics'][cost_type]['Unit']}"
                )
            g1_g2_map[tuple(g["Keys"])] = g["Metrics"][cost_type]["Amount"]

        for g1_i, g1 in enumerate(sorted(all_values1)):
            if costs[g1_i][0]:
                if costs[g1_i][0] != g1:
                    raise Exception(f"Error: {costs[g1_i][0]} != {g1}")
            else:
                costs[g1_i][0] = g1
            for g2_i, g2 in enumerate(all_values2_sorted):
                try:
                    c = float(g1_g2_map[(g1, g2)])
                    costs[g1_i][g2_i + 1] += c
                    costs[g1_i][-1] += c
                except KeyError:
                    pass

    return header, costs


def costs_to_flat(*, results, group1, group2, cost_type):
    # Unpivoted/flat table with columns, no aggregation is done
    header = ["START", "END", group1, group2, "COST"]
    flat_costs = []

    for result in results:
        for g in result["Groups"]:
            if g["Metrics"][cost_type]["Unit"] != EXPECTED_UNIT:
                raise RuntimeError(
                    f"Unexpected unit: {g['Metrics'][cost_type]['Unit']}"
                )
            g1, g2 = g["Keys"]
            start = result["TimePeriod"]["Start"]
            end = result["TimePeriod"]["End"]
            cost = float(g["Metrics"][cost_type]["Amount"])
            flat_costs.append((start, end, g1, g2, cost))

    return header, flat_costs


def _assert_header(header):
    if header[-1] != "TOTAL":
        raise RuntimeError(f"Unexpected header: {header}")


def format_message_summarise(header, group1, costs):
    _assert_header(header)
    costs_dsc = sorted(costs, key=lambda r: r[-1], reverse=True)

    sum_total = 0
    md_rows = ""
    for row in costs_dsc:
        sum_total += row[-1]
        md_rows += f"|{row[0]}|{row[-1]:.2f}|\n"
    msg = (
        f"## Totals: {EXPECTED_UNIT} {sum_total:.2f}\n\n"
        f"|{group1}|Total|\n|-|-|\n{md_rows}"
    )
    return msg


def format_message_all(header, costs, group1, group2, exclude_zero):
    _assert_header(header)

    costs_g1 = sorted(costs, key=lambda r: r[0])

    m = ""
    for row in costs_g1:
        m += f"## {row[0]}\n\n"
        m += f"|{group2}|Cost|\n|-|-|\n"
        for g2, cost in zip(header[1:-1], row[1:-1]):
            if not (exclude_zero and cost == 0):
                m += f"|{g2}|{cost:.2f}|\n"
        m += "\n"
    return m


def costs_to_csv(header, costs):
    s = StringIO()
    writer = csv.writer(s)
    writer.writerow(header)
    writer.writerows(costs)
    return s.getvalue()


def get_raw_cost_data(
    *,
    time_period,
    granularity,
    role_arn,
    regions,
    group1,
    group2,
    exclude_types,
    include_types,
    apply_value_mappings,
):
    session = None
    if role_arn:
        # print(f"Assuming role {role_arn}")
        sts = boto3.client("sts")
        credentials = sts.assume_role(
            RoleArn=role_arn, RoleSessionName="MsTeamsCostBot"
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    results, all_values1, all_values2, value_map1, value_map2 = costs_for_regions(
        time_period=time_period,
        granularity=granularity,
        session=session,
        regions=regions,
        group1=group1,
        group2=group2,
        exclude_types=exclude_types,
        include_types=include_types,
    )

    if apply_value_mappings:
        results, all_values1, all_values2 = _apply_value_mappings(
            results=results,
            all_values1=all_values1,
            all_values2=all_values2,
            value_map1=value_map1,
            value_map2=value_map2,
        )

    return results, all_values1, all_values2, value_map1, value_map2


def _apply_value_mappings(*, results, all_values1, all_values2, value_map1, value_map2):
    """
    Apply value mappings to raw data

    This is mostly for accountname.
    The raw data will have the account number, so replace it with the account name (description).
    """
    if value_map1:
        for result in results:
            for g in result["Groups"]:
                g["Keys"][0] = value_map1[g["Keys"][0]]
        all_values1 = set(value_map1[v] for v in all_values1)
    if value_map2:
        for result in results:
            for g in result["Groups"]:
                g["Keys"][1] = value_map2[g["Keys"][1]]
        all_values2 = set(value_map2[v] for v in all_values2)
    return results, all_values1, all_values2


def create_costs_message(
    *,
    time_period,
    cost_type,
    granularity,
    role_arn,
    regions,
    title_prefix,
    group1,
    group2,
    exclude_types,
    include_types,
    output,
):
    results, all_values1, all_values2, value_map1, value_map2 = get_raw_cost_data(
        time_period=time_period,
        granularity=granularity,
        role_arn=role_arn,
        regions=regions,
        group1=group1,
        group2=group2,
        exclude_types=exclude_types,
        include_types=include_types,
        apply_value_mappings=True,
    )

    header, costs = costs_to_table(
        results=results,
        group1=group1,
        all_values1=all_values1,
        all_values2=all_values2,
        cost_type=cost_type,
    )

    summary = format_message_summarise(header, group1, costs)
    full_costs_split = format_message_all(header, costs, group1, group2, True)

    # Teams message length is limited, so default:
    # - If this is a single AWS account show the summary and breakdown
    # - If there are multiple AWS accounts and a tag is specified just show the tag breakdown combined over all accounts
    # - Otherwise show the AWS account costs only
    if output == "auto":
        if len(all_values1) == 1 or len(all_values2) == 1:
            message = summary + "\n---\n" + full_costs_split
        else:
            message = summary
    elif output == "summary":
        message = summary
    elif output == "full":
        message = full_costs_split
    elif output == "csv":
        message = costs_to_csv(header, costs)
    else:
        raise ValueError(f"Invalid output: {output}")

    days = (
        datetime.fromisoformat(time_period["End"])
        - datetime.fromisoformat(time_period["Start"])
    ).days
    if days > 1:
        title = (
            f"{title_prefix} {time_period['Start']} - {time_period['End']} {cost_type}"
        )
    else:
        weekday = datetime.strftime(datetime.fromisoformat(time_period["Start"]), "%A")
        title = f"{title_prefix} {time_period['Start']} ({weekday}) {cost_type}"

    return message, title


def create_costs_plain_output(
    *,
    time_period,
    cost_type,
    granularity,
    role_arn,
    regions,
    group1,
    group2,
    exclude_types,
    include_types,
    output,
):
    results, all_values1, all_values2, value_map1, value_map2 = get_raw_cost_data(
        time_period=time_period,
        granularity=granularity,
        role_arn=role_arn,
        regions=regions,
        group1=group1,
        group2=group2,
        exclude_types=exclude_types,
        include_types=include_types,
        apply_value_mappings=True,
    )

    if output == "csv":
        header, costs = costs_to_table(
            results=results,
            group1=group1,
            all_values1=all_values1,
            all_values2=all_values2,
            cost_type=cost_type,
        )
    elif output == "flat":
        header, costs = costs_to_flat(
            results=results,
            group1=group1,
            group2=group2,
            cost_type=cost_type,
        )
    else:
        raise ValueError(f"Invalid output for plain output: {output}")
    output = costs_to_csv(header, costs)
    return output


def _str_to_date(s):
    if s and isinstance(s, str):
        s = datetime.fromisoformat(s).date()
    return s


def get_time_period(startdate=None, duration_days=1, enddate=None):
    startdate = _str_to_date(startdate)
    enddate = _str_to_date(enddate)
    if startdate:
        if not enddate:
            enddate = startdate + timedelta(days=duration_days)
    else:
        if not enddate:
            enddate = datetime.now().date()
        startdate = enddate - timedelta(days=duration_days)
    return {"Start": startdate.isoformat(), "End": enddate.isoformat()}
