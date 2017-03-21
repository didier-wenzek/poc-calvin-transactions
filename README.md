POC aimed to evaluate the deterministic approach of Calvin for distributed transactions.
----------------

This POC is based after a series of papers which advocate a deterministic approach for distributed transactions.

* [The Case for Determinism in Database Systems (2010)](http://cs-www.cs.yale.edu/homes/dna/papers/determinism-vldb10.pdf)
* [Calvin: Fast Distributed Transactions for Partitioned Database Systems (2012)](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)
* [Modularity and Scalability in Calvin (2013)](http://cs-www.cs.yale.edu/homes/dna/papers/scalable-calvin.pdf)
* [An Evaluation of the Advantages and Disadvantages of Deterministic Database Systems (2014)](http://www.vldb.org/pvldb/vol7/p821-ren.pdf)

Rather using distributed locks to detect conflicts between transactions processed concurrently,
and having the actual ordering of the transactions driven by race conditions;
Calvin has a deterministic approach, where the order used to resolve conflicts is determined in advance.
* Calvin uses a shared global transaction queue which order rules the transaction processing in a deterministic manner.
* It excludes the sessions of transactions spanning several user commands,
  and it restricts the command set in such a way that the system can statically compute the set of reads and writes of each command.
* Calvin leverages these restrictions to concurrently process non conflicting transactions,
  and processing in the predefined order those with overlapping set of reads and writes.
* Compared to the classic approach with locks, all conflicts are detected in advance;
  and a transaction is never started to be aborted due to a race condition.

The goal of this POC is to evaluate the idea using Kafka both
to implement the transaction log, the response stream and all the streams between the partitions.
* The storage level is an in-memory database ([shopDB.ml](shopDB.ml))
  built along the [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) pattern.
* This database is hard-coded but the scenario is designed to expose issues in distributed transaction:
  integrity constraints and multi-node transactions.
* All the user requests and commands are defined as structured values ([shop.mli](shop.mli)) which vehicle all the relevant data.
  This is also the case for the system responses and all the internal messages (checking a constraint or updating a relation).
* The key point is that the user requests and commands are the *unit* of transaction;
  and that all these transactions are arranged in an ordered sequence.
  The response to a command only depends on the state capturing the previous commands, applied in turn.
  A command triggers a cascade of updates with integrity constraint checks: either all the checks are OK and all the updates must be applied,
  or one the check fails and none of the updates must be applied.

The single node reference implementation takes a stream of user requests and produces a stream of system responses:

```Shell
$ cat example.requests.log
NewCustomer(1, "foo")                      # ok
NewCustomer(2, "bar")                      # ok
NewCategory(0, "default category")         # ok
NewProduct(1, 0, "P1")                     # ok
NewProduct(2, 0, "P2")                     # ok
NewProduct(3, 0, "P3")                     # ok
NewOrder(1,1,[(1,10)])                     # expect error; not enough stock
NewStockDelivery [(1,20),(2,10),(3,100)]   # ok
NewOrder(2,1,[(1,1),(2,5),(3,10)])         # ok
GetStock [1,2,3]

$ cat example.requests.log | ./single_node.native
Accepted(0,NewCustomer(1,"foo"))
Accepted(1,NewCustomer(2,"bar"))
Accepted(2,NewCategory(0,"default category"))
Accepted(3,NewProduct(1,0,"P1"))
Accepted(4,NewProduct(2,0,"P2"))
Accepted(5,NewProduct(3,0,"P3"))
Rejected(6,NewOrder(1,1,[(1,10)]),missing pre-requisites)
Accepted(7,NewStockDelivery([(1,20),(2,10),(3,100)]))
Accepted(8,NewOrder(2,1,[(1,1),(2,5),(3,10)]))
Response(9,Stock([(1,19),(2,5),(3,90)]))
```

The distributed implementation use several databases nodes (using the same in-memory storage as the single node implementation)
but storing only part of the data (using a data distribution scheme described in [shopPartition.ml](shopPartition.ml)).
Two nodes can play the same role and hold the same partition of data, acting as replica.

In this distributed setting,
the nodes can no more simply consume and apply the commands (filtering those relevant to their partition).
* An integrity constraint (say that a product must reference a known category)
  may be resolved only by a node different of the node responsible of the command (say to register a new product).
* An integrity constraint may even span many nodes (this is the case for an order which products may be distributed to different nodes).
* All the nodes must agree (one can not have a node accepting one of two conflicting commands and another node accepting the other command).
  This agreement must be over all the partitions and all the replica.
* The system is expected to leverage the cluster of nodes and to execute the transactions in parallel, at least when there is no conflicts.

This is here that Calvin comes in play.
* A shared queue is used to collect all the transactions.
* The partition nodes may process several transactions concurrently,
  but they never start concurrently two conflicting transactions.
* In case of a conflict, the transaction which comes after in the transaction queue
  is started only when the conflicting one is fully processed (either committed or rejected).
* Conflicts are determined using only the request to determine a set involved keys,
  without reading nor writing the database.
* These sets of keys used by the running transactions are aggregated by each partition node
  to queue transactions waiting a specific set of keys to be available.

The distributed implementation use several Kafka topics (which can be created using [create_topics.sh](create_topics.sh)).
* The topic `db_transaction_request` materializes the transaction log, collecting all the user requests and commands.
  (this topic must have a single partition to ensure a total ordering of the transactions).
* The topic `db_transaction_outcome` collects all the response from the system to the users.
* The topic `db_partition_request` is used to send internal requests to the nodes running the partition.
  This is along this topic that updates, integrity checks and commits are sent.
  This topic has a partition per data partition and are consumed by the partition servers.
* The topic `db_transaction_criteria` is used to gather partial integrity check responses
  in order to trigger the transaction commit or rollback decision.
  In this prototype, these partial responses are collected by a process (called the transaction manager).

To test the distributed implementation we have to launch :
* One transaction manager (the current implementation doesn't support concurrent transaction managers).
* One partition server per partition (the current implementation doesn't support replica).

```Shell
$ xterm -e ./transaction_manager.native 1 1 &

$ xterm -e ./partition_server.native 1 3 &
$ xterm -e ./partition_server.native 2 3 &
$ xterm -e ./partition_server.native 3 3 &
```

Kafka is then used to send transactions and read the system responses.
The outcome MUST be the same as with a single node server.

```Shell
$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic db_transaction_request
NewCustomer(1, "foo")                      # ok
NewCustomer(2, "bar")                      # ok
NewCategory(0, "default category")         # ok
NewProduct(1, 0, "P1")                     # ok
NewProduct(2, 0, "P2")                     # ok
NewProduct(3, 0, "P3")                     # ok
NewOrder(1,1,[(1,10)])                     # expect error; not enough stock
NewStockDelivery [(1,20),(2,10),(3,100)]   # ok
NewOrder(2,1,[(1,1),(2,5),(3,10)])         # ok
GetStock [1,2,3]

$ bin/kafka-console-consumer.sh --zookeeper localhost --topic db_transaction_outcome
Accepted(28,NewCategory(0,"default category"))
Accepted(27,NewCustomer(2,"bar"))
Accepted(30,NewProduct(2,0,"P2"))
Accepted(31,NewProduct(3,0,"P3"))
Accepted(26,NewCustomer(1,"foo"))
Accepted(29,NewProduct(1,0,"P1"))
Accepted(33,NewStockDelivery([(1,20),(2,10),(3,100)]))
Rejected(32,NewOrder(1,1,[(1,10)]),)
Accepted(34,NewOrder(2,1,[(1,1),(2,5),(3,10)]))
Response(35,Stock([(2,5),(3,90),(1,19)]))
```

TODO
----
* Understand why everything is so slow.
  * Is this only due to latency introduced by batch-processing of the requests ?
    Is this latency amplified by the round-trips between the servers ?
  * Give a try to another transport layer between the transaction managers and the transaction servers (ZeroMQ or Nanomsg)
* Add support for concurrent transaction managers.
* Add support for partition replica.

