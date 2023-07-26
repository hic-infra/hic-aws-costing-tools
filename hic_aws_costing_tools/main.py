from argparse import ArgumentParser

from .aws_costs import (
    DEFAULT_COST_TYPE,
    DEFAULT_EXCLUDE_RECORD_TYPES,
    DEFAULT_GRANULARITY,
    create_costs_message,
    create_costs_plain_output,
    get_time_period,
)


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--start", help="Start date (YYYY-MM-DD, inclusive) (default yesterday)"
    )
    parser.add_argument(
        "--end", help="End date (YYYY-MM-DD, exclusive) (default start+1 day)"
    )
    parser.add_argument(
        "--group1",
        default="accountname",
        help=(
            "Group by 'account', 'accountname', 'service' or 'tagname$'. "
            "Tags are indicated by a '$' suffix. "
            "Default accountname."
        ),
    )
    parser.add_argument(
        "--group2",
        default="service",
        help=(
            "Group by 'account', 'accountname', 'service' or 'tagname$'. "
            "Tags are indicated by a '$' suffix. "
            "Default service."
        ),
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
        "--output",
        choices=["auto", "summary", "full", "csv", "flat"],
        default="auto",
        help="Type of message to output",
    )

    args = parser.parse_args()

    time_period = get_time_period(startdate=args.start, enddate=args.end)
    if args.output in ("csv", "flat"):
        message = create_costs_plain_output(
            role_arn=args.assume_role,
            time_period=time_period,
            cost_type=DEFAULT_COST_TYPE,
            granularity=args.granularity.upper(),
            regions=None,
            group1=args.group1,
            group2=args.group2,
            exclude_types=args.exclude_types,
            output=args.output,
        )
    else:
        message, title = create_costs_message(
            role_arn=args.assume_role,
            time_period=time_period,
            cost_type=DEFAULT_COST_TYPE,
            granularity=args.granularity.upper(),
            regions=None,
            title_prefix="Command line test",
            group1=args.group1,
            group2=args.group2,
            exclude_types=args.exclude_types,
            output=args.output,
        )
        print(title)
    print(message)


if __name__ == "__main__":
    main()
