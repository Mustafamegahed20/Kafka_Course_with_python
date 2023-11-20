from confluent_kafka import Producer

def main():
    print("I am a Kafka Producer!")

    # Create producer configuration
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer',
        'acks': 'all',  # This ensures that the producer waits for acknowledgment from all replicas before considering a message as sent.
        'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
        'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'
    }

    # Create producer instance
    producer = Producer(producer_conf)

    # Create a Producer Record
    producer_record = producer.produce('demo_python', key=None, value='hello world')

    # Wait for any outstanding messages to be delivered
    producer.flush()


if __name__ == "__main__":
    main()
