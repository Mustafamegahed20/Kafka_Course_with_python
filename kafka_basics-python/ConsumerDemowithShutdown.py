from confluent_kafka import Consumer, KafkaError
import threading
import signal

def create_consumer():
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-python-application',
        'auto.offset.reset': 'earliest',
    }

    return Consumer(consumer_conf)

def consume_messages(consumer):
    try:
        topic = 'demo_python'
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1000)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Error: {}".format(msg.error()))
                    break

            print('Key: {}, Value: {}, Partition: {}, Offset: {}'.format(msg.key(), msg.value(), msg.partition(), msg.offset()))

    except Exception as e:
        print("Unexpected exception in the consumer:", e)
    finally:
        consumer.close()
        print("Consumer is now gracefully shut down")

def on_shutdown(consumer):
    print("Detected a shutdown, let's exit gracefully...")
    consumer.close()

def main():
    consumer = create_consumer()

    # Get a reference to the main thread
    main_thread = threading.current_thread()

    # Set the shutdown hook
    signal.signal(signal.SIGINT, lambda signum, frame: on_shutdown(consumer))

    # Start consuming messages
    consume_thread = threading.Thread(target=consume_messages, args=(consumer,))
    consume_thread.start()

    # Wait for the consume thread to complete # This line blocks the main thread and waits for the consume_thread to finish its execution before moving on.
    consume_thread.join()

if __name__ == "__main__":
    main()
