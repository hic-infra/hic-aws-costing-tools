#!/usr/bin/env python

from argparse import ArgumentParser
import boto3
import csv
from io import StringIO
import json
import os
import sys
from urllib import request
from datetime import datetime, timedelta


COST_TYPE = "UnblendedCost"
DEFAULT_GRANULARITY = "DAILY"
DEFAULT_EXCLUDE_RECORD_TYPES = ["Credit", "Refund", "Tax"]
EXPECTED_UNIT = "USD"


def costs_for_regions(
    *, time_period, granularity, regions, session, service_or_tag, exclude_types
):
    if session:
        ce = session.client("ce")
    else:
        ce = boto3.client("ce")

    r = ce.get_dimension_values(TimePeriod=time_period, Dimension="LINKED_ACCOUNT")
    all_linked_accounts = dict(
        (dv["Value"], dv["Attributes"]["description"]) for dv in r["DimensionValues"]
    )

    if service_or_tag:
        r = ce.get_tags(TimePeriod=time_period, TagKey=service_or_tag)
        all_services_or_tags = set(f"{service_or_tag}${t}" for t in r["Tags"])
    else:
        r = ce.get_dimension_values(TimePeriod=time_period, Dimension="SERVICE")
        all_services_or_tags = set(dv["Value"] for dv in r["DimensionValues"])

    r = None
    results = []
    kwargs = dict(
        Granularity=granularity,
        GroupBy=[
            {
                "Type": "DIMENSION",
                "Key": "LINKED_ACCOUNT",
            },
        ],
        Metrics=["UnblendedCost"],
        TimePeriod=time_period,
    )
    if service_or_tag:
        kwargs["GroupBy"].append(
            {
                "Type": "TAG",
                "Key": service_or_tag,
            }
        )
    else:
        kwargs["GroupBy"].append(
            {
                "Type": "DIMENSION",
                "Key": "SERVICE",
            }
        )

    exclude_record_types = dict(
        Not=dict(
            Dimensions={
                "Key": "RECORD_TYPE",
                "Values": exclude_types,
            }
        )
    )
    if regions:
        kwargs["Filter"] = dict(
            And=[
                exclude_record_types,
                dict(
                    Dimensions={
                        "Key": "REGION",
                        "Values": regions,
                    }
                ),
            ]
        )
    else:
        kwargs["Filter"] = exclude_record_types

    while not r or "NextPageToken" in r:
        # print(f"get_cost_and_usage({kwargs})")
        r = ce.get_cost_and_usage(**kwargs)
        results.extend(r["ResultsByTime"])

    return results, all_linked_accounts, all_services_or_tags


def costs_to_table(*, results, accounts, services_or_tags, cost_type):
    # results will have one group per day
    # print(json.dumps(results, indent=2, sort_keys=True))

    header = ["ACCOUNT", "ACCOUNT_NAME"] + services_or_tags + ["TOTAL"]
    costs = [[0] * len(header) for _ in range(len(accounts))]

    for result in results:
        acc_svc_map = {}
        for g in result["Groups"]:
            # [account, service-or-tag]
            assert g["Metrics"][cost_type]["Unit"] == EXPECTED_UNIT
            acc_svc_map[tuple(g["Keys"])] = g["Metrics"][cost_type]["Amount"]

        for acc_i, acc in enumerate(sorted(accounts.keys())):
            if costs[acc_i][0] and costs[acc_i][0] != acc:
                raise Exception(f"Error: {costs[acc_i][0]} != {acc}")
            if costs[acc_i][1] and costs[acc_i][1] != accounts[acc]:
                raise Exception(f"Error: {costs[acc_i][1]} != {accounts[acc]}")
            costs[acc_i][0] = acc
            costs[acc_i][1] = accounts[acc]
            for svc_i, svc in enumerate(services_or_tags):
                try:
                    c = float(acc_svc_map[(acc, svc)])
                    costs[acc_i][svc_i + 2] += c
                    costs[acc_i][-1] += c
                except KeyError:
                    pass

    return header, costs


def sum_cost_table_over_accounts(header, costs):
    costs_sum = [0] * len(header)
    costs_sum[0] = costs_sum[1] = "*"
    for row in costs:
        for i, c in enumerate(row[2:]):
            costs_sum[i + 2] += c
    return [costs_sum]


def format_message_summarise(header, costs):
    assert header[1] == "ACCOUNT_NAME"
    assert header[-1] == "TOTAL"
    costs_dsc = sorted(costs, key=lambda r: r[-1], reverse=True)

    sum_total = 0
    md_rows = ""
    for row in costs_dsc:
        sum_total += row[-1]
        md_rows += f"|{row[1]}|{row[-1]:.2f}|\n"
    msg = (
        f"## Account Totals: {EXPECTED_UNIT} {sum_total:.2f}\n\n"
        f"|Account|Total|\n|-|-|\n{md_rows}"
    )
    return msg


def format_message_all(header, costs, service_or_tag, combine_accounts):
    assert header[1] == "ACCOUNT_NAME"
    assert header[-1] == "TOTAL"

    if combine_accounts:
        costs_acc = sum_cost_table_over_accounts(header, costs)
    else:
        # Order by account name
        costs_acc = sorted(costs, key=lambda r: r[1])

    m = ""
    for row in costs_acc:
        m += f"## {row[1]} {row[0]}\n\n"
        m += f"|{service_or_tag or 'Service'}|Cost|\n|-|-|\n"
        for service_or_tag, cost in zip(header[2:-1], row[2:-1]):
            m += f"|{service_or_tag}|{cost:.2f}|\n"
        m += "\n"
    return m


def send_teams(*, message, title, webhook):
    data = {
        #   "@context": "https://schema.org/extensions",
        #   "@type": "MessageCard",
        #   "themeColor": "green",
        "title": title,
        "testformat": "markdown",
        "text": message,
    }
    headers = {"Content-Type": "application/json"}
    bindata = json.dumps(data).encode("utf-8")
    req = request.Request(webhook, bindata, headers)
    resp = request.urlopen(req)
    if resp.status != 200:
        print(resp.reason)
        raise Exception(f"Failed to send message to Teams: {resp.reason}")


def costs_to_csv(header, costs):
    s = StringIO()
    writer = csv.writer(s)
    writer.writerow(header)
    writer.writerows(costs)
    return s.getvalue()


def create_costs_message(
    *,
    time_period,
    cost_type,
    granularity,
    role_arn,
    regions,
    title_prefix,
    service_or_tag,
    exclude_types,
    message_mode,
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

    results, accounts, services_or_tags = costs_for_regions(
        time_period=time_period,
        granularity=granularity,
        session=session,
        regions=regions,
        service_or_tag=service_or_tag,
        exclude_types=exclude_types,
    )

    required_services_or_tags = set(services_or_tags)

    header, costs = costs_to_table(
        results=results,
        accounts=accounts,
        services_or_tags=sorted(required_services_or_tags),
        cost_type=cost_type,
    )

    summary = format_message_summarise(header, costs)
    full_costs_split_acc = format_message_all(header, costs, service_or_tag, False)
    full_costs_sum_acc = format_message_all(header, costs, service_or_tag, True)

    # Teams message length is limited, so default:
    # - If this is a single AWS account show the summary and breakdown
    # - If there are multiple AWS accounts and a tag is specified just show the tag breakdown combined over all accounts
    # - Otherwise show the AWS account costs only
    if message_mode == "auto":
        if len(accounts) == 1:
            message = summary + "\n---\n" + full_costs_split_acc
        elif service_or_tag:
            message = full_costs_sum_acc
        else:
            message = summary
    elif message_mode == "summary":
        message = summary
    elif message_mode == "full":
        message = full_costs_split_acc
    elif message_mode == "csv":
        message = costs_to_csv(header, costs)
    else:
        raise ValueError(f"Invalid message_mode: {message_mode}")

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


def lambda_handler(event, context):
    # Either set a webhook, or a parameter store path containing the webhook
    webhook = os.getenv("MS_TEAMS_WEBHOOK")
    if not webhook:
        ms_teams_parameter_store = os.getenv("MS_TEAMS_PARAMETER_STORE")
        if not ms_teams_parameter_store:
            raise Exception("MS_TEAMS_WEBHOOK or MS_TEAMS_PARAMETER_STORE must be set")
        ssm = boto3.client("ssm")
        webhook = ssm.get_parameter(Name=ms_teams_parameter_store, WithDecryption=True)[
            "Parameter"
        ]["Value"]

    if "start" in event:
        time_period = get_time_period(event["start"], 1)
    else:
        time_period = get_time_period(None, 1)
    if "tag" in event:
        service_or_tag = event["tag"]
    else:
        service_or_tag = None
    cost_explorer_role_arn = os.getenv("COST_EXPLORER_ROLE_ARN")
    title_prefix = os.getenv("MS_TEAMS_TITLE_PREFIX")
    granularity = DEFAULT_GRANULARITY

    message, title = create_costs_message(
        role_arn=cost_explorer_role_arn,
        time_period=time_period,
        cost_type=COST_TYPE,
        granularity=granularity,
        regions=None,
        title_prefix=title_prefix,
        service_or_tag=service_or_tag,
        exclude_types=DEFAULT_EXCLUDE_RECORD_TYPES,
        message_mode="auto",
    )
    send_teams(
        message=message,
        title=title,
        webhook=webhook,
    )
    return {"statusCode": 200, "body": json.dumps(message)}


def main(args):
    parser = ArgumentParser()
    parser.add_argument(
        "--start", help="Start date (YYYY-MM-DD, inclusive) (default yesterday)"
    )
    parser.add_argument(
        "--end", help="End date (YYYY-MM-DD, exclusive) (default start+1 day)"
    )
    parser.add_argument(
        "--tag", help="Group by this tag instead of service (default service)"
    )
    parser.add_argument(
        "--assume-role", help="Optionally assume this role ARN to query Cost Explorer"
    )
    parser.add_argument(
        "--granularity",
        choices=["monthly", "daily"],
        default=DEFAULT_GRANULARITY.lower(),
        help="Fetch costs monthly or daily",
    )
    parser.add_argument(
        "--exclude-types",
        nargs="*",
        default=DEFAULT_EXCLUDE_RECORD_TYPES,
        help=f"Exclude these record types (default {DEFAULT_EXCLUDE_RECORD_TYPES})",
    )
    parser.add_argument(
        "--message-mode",
        choices=["auto", "summary", "full", "csv"],
        default="auto",
        help="Type of message to output",
    )

    args = parser.parse_args()

    time_period = get_time_period(startdate=args.start, enddate=args.end)
    message, title = create_costs_message(
        role_arn=args.assume_role,
        time_period=time_period,
        cost_type=COST_TYPE,
        granularity=args.granularity.upper(),
        regions=None,
        title_prefix="Command line test",
        service_or_tag=args.tag,
        exclude_types=args.exclude_types,
        message_mode=args.message_mode,
    )
    print(title)
    print(message)


if __name__ == "__main__":
    main(sys.argv[1:])
