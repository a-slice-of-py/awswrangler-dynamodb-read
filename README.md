# awswrangler-dynamodb-read

## A candidate for `wr.dynamodb.read_items`

I recently found myself putting some effort in trying to handle reading items from a DynamoDB table and returning a Pandas Dataframe. Basically, I wanted to abstract some complexity away from available Boto3 read actions, and handle once for all the headache of thinking about keys, query, scan, etc.: since I'm pretty happy with the results, I decided to open-sourcing it here while it's being evaluated as candidate for `wr.dynamodb.read_items`.

Since the code should be auto-consistent, if this won't be included in awswrangler stable codebase it's probable that sooner or later I will decide to package it and publish to pip (I tried to stick to awswrangler best practices as much as I can, so it should be an handy drop-in replacement).

I am aware of the recent addition of `wr.dynamodb.read_partiql_query` in aws/aws-sdk-pandas#1390, as well as its currently issue as reported in aws/aws-sdk-pandas#1571, but the below proposed solution does not involve PartiQL: my goal was to avoid as much as possible the risks that come with its usage towards a DynamoDB table, regarding possible translation of a given query to a full scan op (see for example the disclaimer in the [docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.select.html)).

## Features

- [x] automatically switch between available DynamoDB read actions, choosing the optimal one as defined in this hierarchy `get_item > batch_get_item > query > scan` (inspiration from [here](https://dynobase.dev/dynamodb-scan-vs-query/) and [here](https://github.com/bykka/dynamoit))
- [x] handle response pagination ("manually", without using [paginators](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#paginators))
- [x] handle possible `UnprocessedKeys` in `batch_get_item` response
- [x] support condition expressions both as `string` and `Key/Attr` from `boto3.dynamodb.conditions`
- [x] automatically handles botocore client error involving [DynamoDB reserved keywords](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html) via `ExpressionAttributeNames` substitutions
- [x] prevent unwanted full table scan thanks to the `allow_full_scan` kwarg which defaults to `False`
- [x] allow attributes selection via `columns` kwarg, which corresponds to Boto3 `ProjectionExpression`
- [x] support both strongly and eventually consistent reads with the `consistent` kwarg, which defaults to `False`
- [x] support limiting the number of returning items with the `max_items_evaluated` kwarg _(a kind of an `head()` method for the table!)_
- [ ] returning consumed read capacity units
- [ ] anything related to local or global secondary table index
- [ ] parallel scan

The last features are unchecked because I considered them out of scope, at least for the moment.

## Examples

Reading 5 random items from a table

```python
import awswrangler as wr
df = wr.dynamodb.read_items(table_name='my-table', max_items_evaluated=5)
```

Strongly-consistent reading of a given partition value from a table

```python
import awswrangler as wr
df = wr.dynamodb.read_items(table_name='my-table', partition_values=['my-value'], consistent=True)
```

Reading items pairwise-identified by partition and sort values, from a table with a composite primary key

```python
import awswrangler as wr
df = wr.dynamodb.read_items(
    table_name='my-table',
    partition_values=['pv_1', 'pv_2'],
    sort_values=['sv_1', 'sv_2']
)
```

Reading items while retaining only specified attributes, automatically handling possible collision with DynamoDB reserved keywords

```python
import awswrangler as wr
df = wr.dynamodb.read_items(
    table_name='my-table', 
    partition_values=['my-value'], 
    columns=['connection', 'other_col'] # connection is a reserved keyword, managed under the hood!
)
```

Reading all items from a table explicitly allowing full scan

```python
import awswrangler as wr
df = wr.dynamodb.read_items(table_name='my-table', allow_full_scan=True)
```

Reading items matching a KeyConditionExpression expressed with boto3.dynamodb.conditions.Key

```python
import awswrangler as wr
from boto3.dynamodb.conditions import Key
df = wr.dynamodb.read_items(
    table_name='my-table',
    key_condition_expression=Key('key_1').eq('val_1') and Key('key_2').eq('val_2')
)
```

Same as above, but with KeyConditionExpression as string

```python
import awswrangler as wr
df = wr.dynamodb.read_items(
    table_name='my-table',
    key_condition_expression='key_1 = :v1 and key_2 = :v2',
    expression_attribute_values={':v1': 'val_1', ':v2': 'val_2'},
)
```

Reading items matching a FilterExpression expressed with boto3.dynamodb.conditions.Attr

```python
import awswrangler as wr
from boto3.dynamodb.conditions import Attr
df = wr.dynamodb.read_items(
    table_name='my-table',
    filter_expression=Attr('my_attr').eq('this-value')
)
```

Same as above, but with FilterExpression as string

```python
import awswrangler as wr
df = wr.dynamodb.read_items(
    table_name='my-table',
    filter_expression='my_attr = :v',
    expression_attribute_values={':v': 'this-value'}
)
```

Reading items involving an attribute which collides with DynamoDB reserved keywords

```python
import awswrangler as wr
df = wr.dynamodb.read_items(
    table_name='my-table',
    filter_expression='#operator = :v',
    expression_attribute_names={'#operator': 'operator'},
    expression_attribute_values={':v': 'this-value'}
)
```

## Package versions used for testing

I tested all the above-checked features with relatively simple dummy tables, so **it probably requires more focused test sessions**. Anyway, all the tests have been done with Python 3.10.8 and following package versions:

```text
pandas==1.5.2
botocore==1.29.16
boto3==1.26.16
awswrangler==2.17.0
```
