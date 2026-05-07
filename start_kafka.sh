#!/bin/bash
KAFKA_HOME=~/kafka_2.12-3.7.0

echo "Arrancando Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
ZK_PID=$!

sleep 5

echo "Arrancando Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > /tmp/kafka.log 2>&1 &
KAFKA_PID=$!

echo "Zookeeper PID: $ZK_PID | Kafka PID: $KAFKA_PID"
echo "Logs en /tmp/zookeeper.log y /tmp/kafka.log"
echo "Para parar: kill $ZK_PID $KAFKA_PID"