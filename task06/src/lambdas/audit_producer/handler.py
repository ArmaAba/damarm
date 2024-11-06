from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json
import boto3
import uuid
from datetime import datetime
import os

_LOG = get_logger("AuditProducer-handler")
dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("target_table", "Audit")
audit_table = dynamodb.Table(table_name)


class AuditProducer(AbstractLambda):

    def validate_request(self, event) -> dict:
        _LOG.info("Validating request: %s", json.dumps(event))
        pass

    def handle_request(self, event, context):
        _LOG.info("Handling request with event: %s", json.dumps(event))

        for record in event.get('Records', []):
            if record.get('eventName') in ['INSERT', 'MODIFY']:
                try:
                    # Extract the primary key (itemKey)
                    item_key = record['dynamodb']['Keys']['key']['S']
                    modification_time = datetime.utcnow().isoformat()

                    # Initialize variables
                    old_value = None
                    new_value = {}
                    updated_attribute = None

                    _LOG.info("Processing record with eventName %s and itemKey %s", record['eventName'], item_key)

                    # Handle INSERT events
                    if record['eventName'] == 'INSERT':
                        # For a new item, capture 'key' and 'value' as nested fields within newValue
                        new_key = record['dynamodb'].get('NewImage', {}).get('key', {}).get('S')
                        new_value_field = record['dynamodb'].get('NewImage', {}).get('value', {}).get('N')

                        new_value = {
                            'key': new_key,
                            'value': int(new_value_field) if new_value_field else None
                        }

                        _LOG.debug("New item added with newValue: %s", new_value)

                    # Handle MODIFY events
                    elif record['eventName'] == 'MODIFY':
                        # Extract old and new values for 'value' attribute
                        old_value_map = record['dynamodb'].get('OldImage', {}).get('value', {}).get('N')
                        new_value_map = record['dynamodb'].get('NewImage', {}).get('value', {}).get('N')

                        # Check if there's an actual change in 'value' attribute
                        if old_value_map != new_value_map:
                            updated_attribute = 'value'
                            old_value = int(old_value_map) if old_value_map else None
                            new_value = {
                                'key': item_key,
                                'value': int(new_value_map) if new_value_map else None
                            }

                        _LOG.debug("Old value: %s, New value: %s", old_value, new_value)

                    # Construct the audit item
                    audit_item = {
                        'id': str(uuid.uuid4()),
                        'itemKey': item_key,
                        'modificationTime': modification_time,
                        'newValue': new_value
                    }

                    # Include oldValue and updatedAttribute only for MODIFY events
                    if record['eventName'] == 'MODIFY' and updated_attribute:
                        audit_item['updatedAttribute'] = updated_attribute
                        audit_item['oldValue'] = old_value

                    _LOG.debug("Audit item to store: %s", audit_item)

                    # Store the audit entry in DynamoDB
                    self.store_audit_entry(audit_item)

                except Exception as e:
                    _LOG.error("Error processing record: %s. Exception: %s", record, str(e), exc_info=True)

        _LOG.info("Completed processing stream events successfully")
        return {
            'statusCode': 200,
            'body': json.dumps('Processed stream events successfully')
        }


HANDLER = AuditProducer()


def lambda_handler(event, context):
    _LOG.info("Lambda handler invoked")
    return HANDLER.lambda_handler(event=event, context=context)
