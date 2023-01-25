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

Costs for December 2022, split by AWS account and cost tag `Proj`, using an IAM role that can access all AWS accounts in the organisation

```
aws-costs --start 2022-12-01 --end 2023-01-01 --tag Proj \
  --assume-role arn:aws:iam::012345678901:role/Organisation-CostExplorer-role \
  --granularity monthly --message-mode full
```

Use `--message-mode csv` to get a CSV file with the costs.
More options:

```
aws-costs --help
```

## Library

```python
from hic_aws_costing_tools import aws_costs
```

And see the code in [`hic_aws_costing_tools/aws_costs.py`](hic_aws_costing_tools/aws_costs.py).
Docstrings will be added in the future.
