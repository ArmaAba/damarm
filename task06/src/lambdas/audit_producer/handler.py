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
        pass
        
    def handle_request(self, event, context):
        for record in event['Records']:
            if record['eventName'] in ['INSERT', 'MODIFY']:
                item_key = record['dynamodb']['Keys']['id']['S']
                modification_time = datetime.utcnow().isoformat()
                old_value = None
                new_value = {}
                if record['eventName'] == 'MODIFY':
                    old_value = record['dynamodb'].get('OldImage', {})
                    new_value = record['dynamodb'].get('NewImage', {})
                audit_item = {
                    'key': str(uuid.uuid4()),  # Generate a new unique ID for the audit item
                    'itemKey': item_key,
                    'modificationTime': modification_time,
                    'newValue': new_value
                }
                if old_value:
                    updated_attribute = self.find_updated_attribute(old_value, new_value)
                    audit_item['updatedAttribute'] = updated_attribute
                    audit_item['oldValue'] = old_value
                self.store_audit_entry(audit_item)
        return {
            'statusCode': 200,
            'body': json.dumps('Processed stream events successfully')
        }

    def find_updated_attribute(self, old_value, new_value):
        """
        Helper function to find which attribute was updated based on the old and new values.
        """
        for key in new_value:
            if new_value[key] != old_value.get(key):
                return key
        return None

    def store_audit_entry(self, audit_item):
        """
        Helper function to store the audit entry in the DynamoDB 'Audit' table.
        """
        try:
            audit_table.put_item(Item=audit_item)
            print(f"Audit entry stored: {audit_item}")
        except Exception as e:
            print(f"Error storing audit entry: {str(e)}")
            raise e

HANDLER = AuditProducer()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
