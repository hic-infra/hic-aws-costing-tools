# HIC AWS Costing Tools

[![Test](https://github.com/hic-infra/hic-aws-costing-tools/actions/workflows/test.yml/badge.svg)](https://github.com/hic-infra/hic-aws-costing-tools/actions/workflows/test.yml)

Tools used by [Health Informatics Centre (HIC)](https://www.dundee.ac.uk/hic) to calculate AWS costs.

## Installation

```
pip install git+https://github.com/hic-infra/hic-aws-costing-tools
```

## Usage examples

Costs for yesterday, split by service

```
aws-costs
```

Costs can be grouped by two dimensions/tags.
The default is Account name (`--group1 AccountName`) and Service (`--group2 Service`).

For examples, costs for December 2022, split by AWS account name and cost tag `Proj`, using an IAM role that can access all AWS accounts in the organisation.
Note that this utility requires tags to be suffixed with `$` to differentiate them from other dimensions.

```
aws-costs --start 2022-12-01 --end 2023-01-01 --group2 'Proj$' \
  --assume-role arn:aws:iam::012345678901:role/Organisation-CostExplorer-role \
  --granularity monthly --output full
```

Use `--output csv` to get a CSV file with the costs.
If you want to import this data into a tool like PowerBI that expects key-value inputs use `--output flat`.

### More options and examples:

```
aws-costs --help
```

Generate a markdown report for costs grouped by `Proj` and `CreatedBy` cost allocation tags, as a CSV:

```
aws-costs --group1 'Proj$' --group2 'CreatedBy$' \
  --output full --start 2023-06-16 --end 2023-07-16
```

Generate a day by day report for costs grouped by `Proj` and `CreatedBy` (default granularity is monthly):

```
aws-costs --group1 'Proj$' --group2 'CreatedBy$' \
  --output flat --start 2023-06-16 --end 2023-07-16 --granularity daily
```

## Library

```python
from hic_aws_costing_tools import aws_costs
```

And see the code in [`hic_aws_costing_tools/aws_costs.py`](hic_aws_costing_tools/aws_costs.py).
Docstrings will be added in the future.
