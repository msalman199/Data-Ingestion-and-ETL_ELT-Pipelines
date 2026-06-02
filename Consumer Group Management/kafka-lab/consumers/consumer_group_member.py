from kafka import KafkaConsumer
import sys
import time

def create_consumer(consumer_id, group_id):
    """
    Create and configure a Kafka consumer.
    
    Args:
        consumer_id: Unique identifier for this consumer
        group_id: Consumer group name
    
    Returns:
        KafkaConsumer instance
    """
    # TODO: Initialize KafkaConsumer with:
    # - topic: 'orders'
    # - bootstrap_servers
    # - group_id
    # - auto_offset_reset='earliest'
    # - enable_auto_commit=True
    # - consumer_timeout_ms=1000
    pass

def consume_messages(consumer, consumer_id):
    """
    Consume and process messages from Kafka.
    
    Args:
        consumer: KafkaConsumer instance
        consumer_id: Identifier for logging
    """
    print(f"[Consumer {consumer_id}] Starting to consume messages...")
    
    try:
        for message in consumer:
            # TODO: Print message details including:
            # - partition
            # - offset
            # - key
            # - value
            pass
    except KeyboardInterrupt:
        print(f"\n[Consumer {consumer_id}] Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    group_id = "order-processing-group"
    
    consumer = create_consumer(consumer_id, group_id)
    consume_messages(consumer, consumer_id)
