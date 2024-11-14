from tests.test_api_handler import ApiHandlerLambdaTestCase


class TestSuccess(ApiHandlerLambdaTestCase):

    def test_success(self):
        self.assertEqual(self.HANDLER.handle_request(dict(), dict()), 200)

