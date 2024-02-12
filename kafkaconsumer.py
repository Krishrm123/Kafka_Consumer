from confluent_kafka import Consumer, KafkaError
import logger as lg
import os


logger = lg.get_logger("INSIDE main :")

class KafkaConsumerAPI:
    def __init__(self, brokers, group_id, topics):
        self.consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_BROKERS', brokers),
            'group.id': os.environ.get('KAFKA_GROUP_ID', group_id),
            'auto.offset.reset': 'earliest'  
        })
        self.topics = topics
    
    def consume_messages(self):
        messages = [] 
        try:
            self.consumer.subscribe(self.topics)
            logger.debug('The Subscribe to topic completed successfully')
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    logger.debug('There is no messages in the mentioned topics so exit fromthe code')
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        logger.debug('There is error',msg.error())
                        break
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                messages.append(msg.value().decode('utf-8'))
                self.consumer.commit()
                logger.debug('commit occured')
            
        except Exception as err:
            logger.error(err)
            raise ValueError('The Job Failed due to the following error', str(err))
        finally:
            self.consumer.close()
        return messages

if __name__ == "__main__":
    brokers = 'localhost:9092'
    group_id = 'test-consumer-group'
    topics = ['test','my_topic']  
    
    consumer_api = KafkaConsumerAPI(brokers, group_id, topics)
    consumer_api.consume_messages()
