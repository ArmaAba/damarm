from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json

_LOG = get_logger('SqsHandler-handler')


class SqsHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        _LOG.info("Received event: %s", json.dumps(event))

        # Process each record in the SQS event
        for record in event.get('Records', []):
            message_body = record['body']
            _LOG.info("Message body: %s", message_body)
        return 200
    

HANDLER = SqsHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
