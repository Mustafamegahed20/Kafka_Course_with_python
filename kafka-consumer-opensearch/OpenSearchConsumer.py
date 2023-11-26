from opensearchpy import OpenSearch
from opensearch_dsl import Search
import json
from confluent_kafka import Consumer, KafkaError
import atexit
import time


def create_opensearch_client():
    host = 'localhost'
    port = 9200
    return OpenSearch(hosts = [{'host': host, 'port': port}])
def create_kafka_consumer():
    # Replace '127.0.0.1:9092' with your Kafka broker details
    conf = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'consumer-opensearch-demo',
        'auto.offset.reset': 'latest',

    }
    return Consumer(conf)

def extract_id(json_data):
    return json_data['meta']['id']

def main():
    opensearch_client = create_opensearch_client()
    kafka_consumer = create_kafka_consumer()

    def exit_handler():
        print("Detected a shutdown, closing resources...")
        kafka_consumer.close()
        opensearch_client.transport.close()

    atexit.register(exit_handler)

    # Check if the index exists, create if not
    index_exists = opensearch_client.indices.exists(index='wikimedia')
    if not index_exists:
        opensearch_client.indices.create(index='wikimedia')
        print("The Wikimedia Index has been created!")
    else:
        print("The Wikimedia Index already exists")

    kafka_consumer.subscribe(['wikimedia.recentchange'])

    try:
        while True:
            msg = kafka_consumer.poll(timeout=1000)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            try:
                record_value = json.loads(msg.value().decode('utf-8'))
                id_value = extract_id(record_value)
                opensearch_client.index(index='wikimedia', body=record_value, id=id_value)
                print(f"Record with ID {id_value} inserted into OpenSearch.")
            except Exception as e:
                print(f"Error processing record: {str(e)}")

    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()

if __name__ == "__main__":
    main()