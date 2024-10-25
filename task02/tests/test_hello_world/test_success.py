from tests.test_hello_world import HelloWorldLambdaTestCase


class TestSuccess(HelloWorldLambdaTestCase):

    def test_success(self):
        event = {
            "rawPath": "/hello",
            "requestContext": {
                "http": {
                    "method": "GET"
                }
            }
        }
        context = {}
        response = self.HANDLER.handle_request(event, context)
        self.assertEqual(response['statusCode'], 200)  # Check for statusCode in response
        self.assertIn("Hello from Lambda", response['body'])


