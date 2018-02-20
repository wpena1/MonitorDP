# MonitorDP
# Table of Contents
1. [Introduction](README.md#introduction)
2. [Algorithm](README.md#algorithm)

# Introduction
DDoS attacks are extremely harmful and difficult to mitigate. Thus, it is helpful to have a tool that can detect these type of attacks in real time and aids in maintaining availability during attacks.
MonitorDP is a distributed data pipeline that applies a simple algorithm to detect DDoS attacks in realtime.
Requests are sent to a Kafka cluster, these requests are consumed by Flink, and the results of the processing are persisted to a Cassandra DB and monitoring messages are sent to a monitor Kafka topic.

Incoming data is processed with Flink 1.4 using the Java API.
Kafkaconsumer.java is the source code for the Flink job that reads messages from a Kafka topic, applies the simple detection
algorithm and sends an attack message to a different Kafka topic (i.e ddos-topic) when an ddos attack is detected.
Additionally, when the attack flag is set a second Kafka cluster is used to receive all requests that have been marked as
"good" by the Flink job.
# Algorithm

- For every incoming request, 
  - If there are any attacking requests
    - check that incoming request is not flagged
    - if it’s flagged, do not process.
    - if it’s not flagged send downstream.
  - If there are not any attacking requests, send request downstream to be counted.

## Counting  

- Count requests in an specified window
  - If the count is above a specified threshold, flag the request
  - When the number of flagged requests is above a specified threshold, set DDoS attack and send attack message to notification message queue
