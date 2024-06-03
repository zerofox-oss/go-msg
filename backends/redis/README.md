# Redis Stream Implemenation

## Architecture Overview

* A **Stream** is equivalent to an SNS topic. 

* **Consumer groups** are equivalent to an SNS topic subscription. 
  A **stream** may have multiple consumer groups (subscribers).

    > Note, when creating a consumer group, you can choose whether to forward all messages in the stream, or only new ones
    > by specifying a message ID. "0" means send all, "$" means only send new.

* **Consumers** are individual processes that can receive messages from a stream. 
  A service which scales horizontally will have multiple consumers reading from the same stream.
  
  > Note, consumers will use XREADGROUP to read messages from a stream. Those messages will be assigned to that consumer, 
  > so if the message is not ACKd, then it will be received again by the same consumer (no others will have access to it).
  > This ensures that messages are not processed by more than one consumer.
  >
  > There is one slight caveat to this, which is that you can use XCLAIM/XAUTOCLAIM in order to re-assign any messages
  > on the PEL (Pending Entries List) to another consumer.

## Example: Multiple Consumers

This example:

* Creates a stream with multiple consumer groups (3)
* Creates 2 consumers for one of the groups (`group1`)
* Writes 2 messages to the stream
* Once the messages are processed, they will not be processed again (by the same or the other consumer).
* The other groups will still have 2 messages to be assigned (`group2`, `group3`).

```bash
$ cd backends/redis
$ go run .
2024/06/24 14:03:05 === Test #1 - Multiple Consumers of a Stream
2024/06/24 14:03:05 Stream=stream1 has messages=2, consumer_groups=2
2024/06/24 14:03:05 XInfoGroups: [{Name:group1 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:06 XInfoGroups: [{Name:group1 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:07 Returning without error to ACK.
2024/06/24 14:03:07 Returning without error to ACK.
2024/06/24 14:03:07 XInfoGroups: [{Name:group1 Consumers:1 Pending:2 LastDeliveredID:1719252185290-0 EntriesRead:2 Lag:0} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:08 XInfoGroups: [{Name:group1 Consumers:1 Pending:0 LastDeliveredID:1719252185290-0 EntriesRead:2 Lag:0} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:09 XInfoGroups: [{Name:group1 Consumers:1 Pending:0 LastDeliveredID:1719252185290-0 EntriesRead:2 Lag:0} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:10 XInfoGroups: [{Name:group1 Consumers:1 Pending:0 LastDeliveredID:1719252185290-0 EntriesRead:2 Lag:0} {Name:group2 Consumers:0 Pending:0 LastDeliveredID:0-0 EntriesRead:0 Lag:2}]
2024/06/24 14:03:11 Processed all messages!
```

## Example: Reassign messages 


## TODO

* I need to figure out how to receive PENDING messages which were not processed by a consumer.
  

* Despite ACKing messages, they will not be removed from the stream. Which means Redis will grow unbounded.
  To resolve, we could run XTRIM on a cron (Say 3d) which will remove any messages older than that period.
  This is effectively creating a message retention period.

*  
