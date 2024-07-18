---
Date: 2024/07/08
Authors: @Xopherus, @shezadkhan137
Status: Accepted
---

# Multi-Server Fair Weighted Queues

## Context

ZeroFox needs to prioritize messages within SQS queues, especially when services receive data from multiple sources with varying latency requirements. Prioritization is key to meeting SLAs for high-priority messages while ensuring lower-priority messages are not starved. Additionally, we need to dynamically allocate throughput based on message priority to ensure fairness and efficiency.

## Decisions

1. Implement an experimental Weighted Fair Queue algorithm as a decorator for the go-msg library (receiver). 
This decorator, called `WeightedFairReceiver`, will enable message prioritization based on assigned weights.

2. Implement a `MultiServer` wrapper around `WeightedFairReceiver`. 
This server will consume messages from multiple underlying servers into a single receiver, guaranteeing consumption in proportion to assigned weights.
