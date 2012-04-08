# Introduction 

A Transaction is a group of operations combined into a logical unit of work. Developer uses transactions to control and maintain the consistency and integrity of each action in a transaction, despite errors that might occur inthe system either due to concurrency or hardware failure. In database context a transaction on a database is considered to be set of operations performed such that database is in a consistent state before and after the transaction and in case of failure the system must provide ways to rollback partial transactions to bring the database back into a consistent state. Transaction in the database environment mainly serve two purposes :

1. To provide reliable units of work that allow correct recovery from failures and keep a database consistent even in cases of system failure, when execution stops (completely or partially) and many operations upon a database remain uncompleted, with unclear status.
2. To provide isolation between programs accessing a database concurrently. If this isolation is not provided the programs outcome are possibly erroneous.

Transactions provide an "all-or-nothing" proposition, stating that each work-unit performed in a database must either complete in its entirety or have no effect whatsoever. Further, the system must isolate each transaction from other transactions, results must conform to existing constraints in the database, and transactions that complete successfully must get written to durable storage. Thus , even in case of hardware failure a transaction once commited must persist. A transaction is expected satisfy ACID guarantees. 

* **Atomicity** means a transaction can end only in two ways : either successfully , in which case all its effects are written to a durable storage and persist between power failures , or unsuccessfully , in which case it has no effect , it can be assumed that this transaction never happened. 
* **Consistency** just means that a transaction is written correctly, so that when it completes successfully, the database is in a consistent state.
* **Isolation** means that the transaction appears to execute completely alone, even if, in fact, other transactions are running simultaneously. In other words, transaction always sees a consistent snapshot of the database and is totally unaware of the changes made by other transactions which are running concurrently with it. 
* **Durability** means that a successful transaction's changes are permanent and persist in all possible sorts of failures. In practice this means to be written on disk. 
