from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json

_LOG = get_logger('SnsHandler-handler')


class SnsHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        # Perform any necessary validation for the SNS event here
        pass

    def handle_request(self, event, context):
        _LOG.info("Received SNS event: %s", json.dumps(event))

        # Process each record in the SNS event
        for record in event.get('Records', []):
            sns_message = record.get('Sns', {}).get('Message', '')
            _LOG.info("SNS Message: %s", sns_message)
        return 200


HANDLER = SnsHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
