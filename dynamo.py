from functools import wraps
from typing import Any, List, Mapping, Optional, Sequence

import awswrangler as wr
import boto3
import pandas as pd
from awswrangler import _utils
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.table import TableResource
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
def _read_items(table: TableResource,
                boto3_session: Optional[boto3.Session] = None,
                **kwargs
                ) -> Sequence:
    client = _utils.client(service_name='dynamodb', session=boto3_session)
    keys = kwargs.pop('Keys', None)
    if keys is not None:
        if len(keys) == 1:
            logger.debug('get_item')
            return [
                table
                .get_item(Key=keys[0], **kwargs)
                .get('Item', dict())
            ]
        else:
            logger.debug('batch_get_item')
            # TODO: manage UnprocessedKeys 
            ...
            ts, tds = TypeSerializer(), TypeDeserializer()
            items = (
                    client.batch_get_item(
                        RequestItems={
                            table.table_name: {
                                'Keys': list(map(lambda x: {k: ts.serialize(v) for k, v in x.items()}, keys)),
                                **kwargs
                            }
                        }
                    )
                    ['Responses']
                    [table.table_name]
            )
            return list(map(lambda x: {k: tds.deserialize(v) for k, v in x.items()}, items))
    else:
        if 'KeyConditionExpression' in kwargs:
            logger.debug('query')
            return (
                table
                .query(**kwargs)
                ['Items']
            )
        else:
            logger.debug('scan')
            return (
                table
                .scan(**kwargs)
                .get('Items')
            )


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

    table = wr.dynamodb.get_table(table_name=table_name, boto3_session=boto3_session)    

    # Get table key schema
    key_schema = table.key_schema

    # Detect sort key, if any
    if len(key_schema) == 1:
        partition_key, sort_key = key_schema[0]['AttributeName'], None
    else:
        partition_key, sort_key = (
            next(filter(lambda x: x['KeyType'] == 'HASH', table.key_schema))[
                'AttributeName'],
            next(filter(lambda x: x['KeyType'] == 'RANGE', table.key_schema))[
                'AttributeName']
        )

    def ensure_coherency():
        if sort_values is None:
            raise ValueError(f'Table {table_name} has {sort_key} as sort key: please provide sort_values.')
        elif len(sort_values) != len(partition_values):
            raise ValueError('Partition and sort values must have the same length.')

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
        items = _read_items(table, boto3_session, **kwargs)
    else:
        raise ValueError(
            f'Please provide at least one between partition_values, sort_values and filter_expression or allow full scan.')

    return pd.DataFrame(items)