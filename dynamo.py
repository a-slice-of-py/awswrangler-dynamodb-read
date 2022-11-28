from functools import wraps
from typing import Any, Callable, Mapping, Optional, Sequence

import awswrangler as wr
import boto3
import pandas as pd
from awswrangler import _utils, exceptions
from boto3.dynamodb.conditions import ConditionBase
from botocore.exceptions import ClientError
from loguru import logger

HANDLED_KWARGS = ('ProjectionExpression', 'KeyConditionExpression', 'FilterExpression')

def get_invalid_kwarg(msg: str) -> Optional[str]:
    """Detect which kwarg contains reserved keywords based on given error message.

    Args:
        msg (str): botocore client error message.

    Returns:
        Detected invalid kwarg if any, None otherwise.
    """
    for kwarg in HANDLED_KWARGS:
        if msg.startswith(f'Invalid {kwarg}: Attribute name is a reserved keyword; reserved keyword: '):
            return kwarg


def handle_boto3_client_error(func: Callable) -> Any:
    """Handle automatic replacement of DynamoDB reserved keywords.
    
    For reserved keywords reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.    
    """
    @wraps(func)
    def wrapper(*args, **kwargs):        
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code, error_message = e.response['Error']['Code'], e.response['Error']['Message']
            # Check catched error to verify its message
            if (error_code == 'ValidationException') and (kwarg := get_invalid_kwarg(error_message)):
                reserved_keyword = error_message.split('keyword: ')[-1]
                sanitized_keyword = f'#{reserved_keyword}'
                kwargs[kwarg] = kwargs[kwarg].replace(
                    reserved_keyword, sanitized_keyword)
                kwargs['ExpressionAttributeNames'] = {
                    **kwargs.get('ExpressionAttributeNames', {}),
                    sanitized_keyword: reserved_keyword
                }
                # SEE: recursive approach guarantees that each reserved keyword will be properly replaced,
                # even if it will require as many calls as the reserved keywords involved (not so efficient...)
                return wrapper(*args, **kwargs)
            # Otherwise raise it
            else:
                raise e
    return wrapper


@handle_boto3_client_error
def _read_items(table_name: str,
                boto3_session: Optional[boto3.Session] = None,
                **kwargs
                ) -> Sequence:
    """Read items from given DynamoDB table.

    This function set the optimal reading strategy based on the received kwargs.

    Args:
        table_name (str): DynamoDB table name.
        boto3_session (Optional[boto3.Session], optional): Boto3 Session. Defaults to None (the default boto3 Session will be used).

    Returns:
        Retrieved items.
    """
    # Get DynamoDB resource and Table instance
    resource = _utils.resource(service_name='dynamodb', session=boto3_session)
    table = wr.dynamodb.get_table(table_name=table_name, boto3_session=boto3_session)

    # Extract 'Keys' from provided kwargs: if needed, will be reinserted later on
    keys = kwargs.pop('Keys', None)

    # Conditionally define optimal reading strategy
    use_get_item = (keys is not None) and (len(keys)==1)
    use_batch_get_item = (keys is not None) and (len(keys) > 1)
    use_query = (keys is None) and ('KeyConditionExpression' in kwargs)
    use_scan = (keys is None) and ('KeyConditionExpression' not in kwargs)

    # Read items
    if use_get_item:
        logger.debug('get_item')
        kwargs['Key'] = keys[0]
        items = [
            table
            .get_item(**kwargs)
            .get('Item', {})
        ]
    elif use_batch_get_item:
        logger.debug('batch_get_item')
        kwargs['Keys'] = keys
        response = resource.batch_get_item(RequestItems={table_name: kwargs})
        items = response.get('Responses', {table_name: []}).get(table_name, [])
        # SEE: handle possible unprocessed keys. As suggested in Boto3 docs, 
        # this approach should involve exponential backoff, but as stated
        # [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff)
        # this should be already managed by AWS SDK itself
        while response['UnprocessedKeys']:                          
            kwargs['Keys'] = response['UnprocessedKeys'][table_name]['Keys']
            response = resource.batch_get_item(RequestItems={table_name: kwargs})
            items.extend(response.get('Responses', {table_name: []}).get(table_name, []))
    elif use_query or use_scan:
        if use_query:
            logger.debug('query')
        else:
            logger.debug('scan')

        _read_method = table.query if use_query else table.scan
        response = _read_method(**kwargs)
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = _read_method(**kwargs)
            items.extend(response.get('Items', []))
    
    return items


def read_items(*,
               table_name: str,
               partition_values: Optional[Sequence[Any]] = None,
               sort_values: Optional[Sequence[Any]] = None,
               filter_expression: Optional[ConditionBase] = None,
               key_condition_expression: Optional[ConditionBase] = None,
               expression_attribute_names: Optional[Mapping] = None,
               expression_attribute_values: Optional[Mapping] = None,
               consistent: bool = False,
               columns: Optional[Sequence] = None,
               allow_full_scan: bool = False,
               max_items_evaluated: Optional[int] = None,
               boto3_session: Optional[boto3.Session] = None
               ) -> pd.DataFrame:
    """Read items from given DynamoDB table.

    This function aims to gracefully handle (some of) the complexity of read actions 
    available in Boto3 towards a DynamoDB table, abstracting it away while providing 
    a single, unified entrypoint.
    
    Under the hood, it wraps all the four available read actions: get_item, batch_get_item,
    query and scan.

    Args:
        table_name (str): DynamoDB table name.
        partition_values (Optional[Sequence[Any]], optional): partition key values to retrieve. Defaults to None.
        sort_values (Optional[Sequence[Any]], optional): sort key values to retrieve. Defaults to None.
        filter_expression (Optional[ConditionBase], optional): filter expression made of boto3.dynamodb.conditions.Attr conditions. Defaults to None.
        key_condition_expression (Optional[ConditionBase], optional): key condition expression made of boto3.dynamodb.conditions.Key conditions. Defaults to None.
        expression_attribute_names (Optional[Mapping], optional): mapping of placeholder and target attributes. Defaults to None.
        expression_attribute_values (Optional[Mapping], optional): mapping of placeholder and target values. Defaults to None.
        consistent (bool, optional): if True, ensure that the performed read operation is strongly consistent, otherwise eventually consistent. Defaults to False.
        columns (Optional[Sequence], optional): attributes to retain in the returned items. Defaults to None (all attributes).
        allow_full_scan (bool, optional): if True, allow full table scan without any filtering. Defaults to False.
        max_items_evaluated (Optional[int], optional): limit the number of items evaluated in case of query or scan operations. Defaults to None (all matching items).
        boto3_session (Optional[boto3.Session], optional): Boto3 Session. Defaults to None (the default boto3 Session will be used).

    Raises:
        exceptions.InvalidArgumentType: when the specified table has also a sort key but only the partition values are specified.
        exceptions.InvalidArgumentCombination: when both partition and sort values sequences are specified but they have different lengths, or when provided parameters
            are not enough informative to proceed with a read operation.

    Returns:
        A Dataframe containing the retrieved items.
    """
    # Extract key schema
    table_key_schema = (
        wr.dynamodb
        .get_table(table_name=table_name, boto3_session=boto3_session)
        .key_schema
    )

    # Detect sort key, if any
    if len(table_key_schema) == 1:
        partition_key, sort_key = table_key_schema[0]['AttributeName'], None
    else:
        partition_key, sort_key = (
            next(filter(lambda x: x['KeyType'] == 'HASH', table_key_schema))[
                'AttributeName'],
            next(filter(lambda x: x['KeyType'] == 'RANGE', table_key_schema))[
                'AttributeName']
        )

    # Handy checker
    def ensure_coherency():
        if sort_values is None:
            raise exceptions.InvalidArgumentType(f'Argument sort_values cannot be None: table {table_name} has {sort_key} as sort key.')
        elif len(sort_values) != len(partition_values):
            raise exceptions.InvalidArgumentCombination('Partition and sort values must have the same length.')

    # Build kwargs shared by read methods
    kwargs = {
        'ConsistentRead': consistent
    }
    if partition_values is not None:
        if sort_key is None:
            keys = [{partition_key: pv} for pv in partition_values]
        else:
            ensure_coherency()
            keys = [{partition_key: pv, sort_key: sv} for pv, sv in zip(partition_values, sort_values)]
        kwargs['Keys'] = keys
    if key_condition_expression is not None:
        kwargs['KeyConditionExpression'] = key_condition_expression
    if filter_expression is not None:
        kwargs['FilterExpression'] = filter_expression
    if columns is not None:
        kwargs['ProjectionExpression'] = ', '.join(columns)
    if expression_attribute_names is not None:
        kwargs['ExpressionAttributeNames'] = expression_attribute_names
    if expression_attribute_values is not None:
        kwargs['ExpressionAttributeValues'] = expression_attribute_values
    if max_items_evaluated is not None:
        kwargs['Limit'] = max_items_evaluated
    
    # If kwargs are sufficiently informative, proceed with actual read op
    if ('Keys' in kwargs) or ('KeyConditionExpression' in kwargs) or ('FilterExpression' in kwargs) or allow_full_scan or max_items_evaluated:
        items = _read_items(table_name, boto3_session, **kwargs)
    # Raise otherwise
    else:
        raise exceptions.InvalidArgumentCombination(
            f'Please provide at least one between partition_values, sort_values, filter_expression, allow_full_scan or max_items_evaluated.')

    # Enforce DataFrame type
    return pd.DataFrame(items)