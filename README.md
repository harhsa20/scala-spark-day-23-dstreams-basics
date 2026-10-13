# Day 23 — DStreams Basics

## Objective
Stream application logs and count ERROR messages every 5 seconds using Spark Streaming DStreams.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- Java 17
- sbt 1.12.15

## Features
- Create a StreamingContext with a 5-second batch interval.
- Read log lines from a TCP socket.
- Apply flatMap, map, and filter transformations.
- Count ERROR messages in each micro-batch.
- Print the count for every batch.

## Run
Start a socket listener in one terminal:
```bash
nc -lk 9999
