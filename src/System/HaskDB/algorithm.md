# Algorithm

Any transaction has access to three operations :

* readblock
* writeblock 
* checkpoint

When a write operation is performed , the change is stored in a separate *journal* file per transaction.When a read operation is performed , it reads from the latest file which contains the block data.

Committing a transaction increments the *file version* of the trasaction file and marks its own journal file as valid.

The set of all valid journal files and the main transaction file together represent the current state of the database at any given point of time.

Transaction would fail/abort when the readblocks of the current transaction conflict with the blocks changed between the start of the current transaction and current *version* .

Write operations always go through without any waiting period and hence they are optimal.
The time complexity of read operation is proportional to the number of journal files on disk.

checkpointing transfers all the data in journal files to the main database file. Hence , frequent checkpointing would improve the overhead of read operations.

There is a tradeoff between average read performance and average write performance. To maximize the read performance, one wants to keep the number of journal files as small as possible and hence run checkpoints frequently. To maximize write performance, one wants to amortize the cost of each checkpoint over as many writes as possible, meaning that one wants to run checkpoints infrequently and let the no of journal files grow as large as possible before each checkpoint. The decision of how often to run checkpoints may therefore vary from one application to another depending on the relative read and write performance requirements of the application.



