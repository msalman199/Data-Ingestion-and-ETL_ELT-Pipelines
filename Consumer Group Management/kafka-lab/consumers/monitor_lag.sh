#!/bin/bash

echo "Monitoring Consumer Group Lag..."
echo "Press Ctrl+C to stop"
echo ""

while true; do
  clear
  date
  echo "================================"
  $KAFKA_HOME/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group order-processing-group \
    --describe
  sleep 5
done
