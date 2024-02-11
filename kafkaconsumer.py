from confluent_kafka import Consumer, KafkaError

class KafkaConsumerAPI:
    def __init__(self, brokers, group_id, topics):
        self.consumer = Consumer({
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'  
        })
        self.topics = topics

    def consume_messages(self):
        try:
            self.consumer.subscribe(self.topics)
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                print('Received message: {}'.format(msg.value().decode('utf-8')))
                self.consumer.commit() # Manually commit offsets
        except Exception as err:
            raise ValueError('The Job Failed due to the following error',str(err)) #handling all exceptions
        finally:
            self.consumer.close()

if __name__ == "__main__":
    brokers = 'localhost:9092'
    group_id = 'test-consumer-group'
    topics = ['test','my_topic'] # consume messages from multiple consumers 
    
    consumer_api = KafkaConsumerAPI(brokers, group_id, topics)
    consumer_api.consume_messages()
