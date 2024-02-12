import unittest
from unittest.mock import patch, MagicMock
from kafka_test import KafkaConsumerAPI

class TestKafkaConsumerAPI(unittest.TestCase):
    @patch('kafka_test.Consumer')
    def setUp(self, mock_consumer):
        self.consumer = KafkaConsumerAPI('localhost:9092', 'test-consumer-group', ['test', 'my_topic'])
        self.mock_consumer = mock_consumer

    def test_consume_messages(self):
        expected_message = 'Test Message'
        message_mock = MagicMock()
        message_mock.error.return_value = None
        message_mock.value.return_value = expected_message.encode('utf-8')
        self.mock_consumer.return_value.poll = MagicMock(side_effect=[message_mock,None])
        
        actual_messages = self.consumer.consume_messages()
        actual_message = actual_messages[0] if actual_messages else None

        
        self.assertEqual(expected_message, actual_message)

    def test_consume_messages_with_error(self):
  
        error_message_mock = MagicMock()
        error_message_mock.error.return_value = "Some error occurred"
        self.mock_consumer.return_value.poll = MagicMock(return_value=error_message_mock)

        
        with self.assertRaises(ValueError) as context:
            self.consumer.consume_messages()

       
        self.mock_consumer.return_value.poll.assert_called_once()
        self.mock_consumer.return_value.close.assert_called_once()
        
        

if __name__ == '__main__':
    unittest.main()



