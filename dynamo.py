from functools import wraps
from typing import Any, Mapping, Optional, Sequence

import awswrangler as wr
import boto3
import pandas as pd
from awswrangler import _utils, exceptions
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.transform import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from loguru import logger


def handle_boto3_client_error(func):

    # TODO: test behaviour with more than one reserved keywords
    ...

    INVALID_PROJECTION_WITH_RESERVED_KEYWORD = 'Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: '

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code, error_message = e.response['Error']['Code'], e.response['Error']['Message']
            if (error_code == 'ValidationException') and (error_message.startswith(INVALID_PROJECTION_WITH_RESERVED_KEYWORD)):
                reserved_keyword = error_message.split(
                    INVALID_PROJECTION_WITH_RESERVED_KEYWORD)[-1]
                sanitized_keyword = f'#{reserved_keyword}'
                kwargs['ProjectionExpression'] = kwargs['ProjectionExpression'].replace(
                    reserved_keyword, sanitized_keyword)
                kwargs['ExpressionAttributeNames'] = {
                    sanitized_keyword: reserved_keyword}
                return func(*args, **kwargs)                
            else:
                raise e
    return wrapper


@handle_boto3_client_error
def _read_items(table_name: str,
                boto3_session: Optional[boto3.Session] = None,
                **kwargs
                ) -> Sequence:
    
    # Get DynamoDB client and Table instance
    client = _utils.client(service_name='dynamodb', session=boto3_session)
    table = wr.dynamodb.get_table(table_name=table_name, boto3_session=boto3_session)

    # Extract 'Keys' from provided kwargs
    keys = kwargs.pop('Keys', None)

    # Conditionally set reading method
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
        # TODO: manage UnprocessedKeys 
        ...
        ts, tds = TypeSerializer(), TypeDeserializer()
        kwargs['Keys'] = list(map(
            lambda x: {k: ts.serialize(v) for k, v in x.items()},
            keys
        ))
        items = list(
            map(
                lambda x: {k: tds.deserialize(v) for k, v in x.items()},
                client.batch_get_item(RequestItems={table_name: kwargs})
                .get('Responses', {table_name: []})
                .get(table_name, [])
            )
        )
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
               allow_full_scan: bool = False,  # security gate, prevents unwanted full scan
               max_items_evaluated: Optional[int] = None,
               boto3_session: Optional[boto3.Session] = None
               ) -> Sequence:

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
        
    if ('Keys' in kwargs) or ('KeyConditionExpression' in kwargs) or ('FilterExpression' in kwargs) or allow_full_scan or max_items_evaluated:
        items = _read_items(table_name, boto3_session, **kwargs)
    else:
        raise exceptions.InvalidArgumentCombination(
            f'Please provide at least one between partition_values, sort_values, filter_expression, allow_full_scan or max_items_evaluated.')

    return pd.DataFrame(items)