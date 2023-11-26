import logging
from confluent_kafka import Producer, KafkaException
from confluent_kafka.serialization import StringSerializer
from sseclient import SSEClient

class WikimediaChangeHandler:
    def __init__(self, kafka_producer, topic):
        self.kafka_producer = kafka_producer
        self.topic = topic

    def on_open(self):
        pass

    def on_closed(self):
        self.kafka_producer.flush()

    def on_message(self, event):
        logging.info(event.data)
        try:
            self.kafka_producer.produce(self.topic, event.data)
        except KafkaException as e:
            logging.error(f"Error producing message to Kafka: {e}")

    def on_error(self, error):
        logging.error(f"Error in Stream Reading: {error}")

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # create Producer Properties
    producer_conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'client.id': 'python-producer',
        'acks': 'all',
        
        
    }

    # create the Producer
    producer = Producer(producer_conf)

    topic = "wikimedia.recentchange"

    event_handler = WikimediaChangeHandler(producer, topic)
    url = "https://stream.wikimedia.org/v2/stream/recentchange"

    # start consuming messages using SSEClient
    client = SSEClient(url)
    try:
        for event in client:
            if event.event == 'message':
                event_handler.on_message(event)
            elif event.event == 'error':
                event_handler.on_error(event.data)
    finally:
        client.close()

if __name__ == "__main__":
    main()
