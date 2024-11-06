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
                    # Get the primary key of the item that triggered the event
                    item_key = record['dynamodb']['Keys']['key']['S']
                    modification_time = datetime.utcnow().isoformat()
                    new_value = None
                    old_value = None
                    updated_attribute = None

                    _LOG.info("Processing record with eventName %s and itemKey %s", record['eventName'], item_key)

                    # For INSERT event, store the entire NewImage in newValue
                    if record['eventName'] == 'INSERT':
                        new_image = record['dynamodb'].get('NewImage', {})
                        new_value = {
                            "key": new_image['key']['S'],
                            "value": int(new_image['value']['N'])
                        }
                        _LOG.debug("New value for INSERT: %s", new_value)

                    # For MODIFY event, store specific old and new values
                    elif record['eventName'] == 'MODIFY':
                        new_image = record['dynamodb'].get('NewImage', {})
                        old_image = record['dynamodb'].get('OldImage', {})

                        # Assuming 'value' is the attribute we're monitoring for changes
                        old_value = int(old_image['value']['N'])
                        new_value = int(new_image['value']['N'])
                        updated_attribute = "value"

                        _LOG.debug("Old value: %s, New value: %s", old_value, new_value)

                    # Construct audit item with appropriate structure
                    audit_item = {
                        'id': str(uuid.uuid4()),  # Generate a new unique ID for the audit item
                        'itemKey': item_key,
                        'modificationTime': modification_time,
                        'newValue': new_value
                    }

                    # Add fields specific to MODIFY events
                    if record['eventName'] == 'MODIFY':
                        audit_item['updatedAttribute'] = updated_attribute
                        audit_item['oldValue'] = old_value

                    _LOG.info("Storing audit entry: %s", audit_item)
                    self.store_audit_entry(audit_item)

                except Exception as e:
                    _LOG.error("Error processing record: %s. Exception: %s", record, str(e), exc_info=True)

        _LOG.info("Completed processing stream events successfully")
        return {
            'statusCode': 200,
            'body': json}

    def find_updated_attribute(self, old_value, new_value):
        """
        Helper function to find which attribute was updated based on the old and new values.
        """
        _LOG.debug("Finding updated attribute between old_value: %s and new_value: %s", old_value, new_value)
        for key in new_value:
            if new_value[key] != old_value.get(key):
                _LOG.info("Found updated attribute: %s", key)
                return key
        _LOG.info("No updated attribute found")
        return None

    def store_audit_entry(self, audit_item):
        """
        Helper function to store the audit entry in the DynamoDB 'Audit' table.
        """
        try:
            _LOG.info("Storing audit entry: %s", audit_item)
            audit_table.put_item(Item=audit_item)
            _LOG.info("Audit entry stored successfully")
        except Exception as e:
            _LOG.error("Error storing audit entry: %s", str(e), exc_info=True)
            raise e


HANDLER = AuditProducer()


def lambda_handler(event, context):
    _LOG.info("Lambda handler invoked")
    return HANDLER.lambda_handler(event=event, context=context)
