# MonitorDP
# Table of Contents
1. [Introduction](README.md#introduction)
2. [Algorithm](README.md#algorithm)

# Introduction
DDoS attacks are difficult to mitigate 
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
