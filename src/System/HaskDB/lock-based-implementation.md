Most of the existing implementations use locks on files for concurrency control.

# A simplified model of database file locking 

From the point of view of a single process, a database file can be in one of five locking states:

* **UNLOCKED** : No locks are held on the database. The database may be neither read nor written. Any internally cached data is considered suspect and subject to verification against the database file before being used. Other processes can read or write the database as their own locking states permit. This is the default state.
* **SHARED** :   The database may be read but not written. Any number of processes can hold SHARED locks at the same time, hence there can be many simultaneous readers. But no other thread or process is allowed to write to the database file while one or more SHARED locks are active.
* **RESERVED** :     A RESERVED lock means that the process is planning on writing to the database file at some point in the future but that it is currently just reading from the file. Only a single RESERVED lock may be active at one time, though multiple SHARED locks can coexist with a single RESERVED lock. RESERVED differs from PENDING in that new SHARED locks can be acquired while there is a RESERVED lock.
* **PENDING** :  A PENDING lock means that the process holding the lock wants to write to the database as soon as possible and is just waiting on all current SHARED locks to clear so that it can get an EXCLUSIVE lock. No new SHARED locks are permitted against the database if a PENDING lock is active, though existing SHARED locks are allowed to continue.
* **EXCLUSIVE** :    An EXCLUSIVE lock is needed in order to write to the database file. Only one EXCLUSIVE lock is allowed on the file and no other locks of any kind are allowed to coexist with an EXCLUSIVE lock.

An example of database engine which follows such model is SQlite.
Multiple readers can read from the database file. 
Only a single writer can write to the database at any given time and all the reader have to wait till the write is complete.
Hence such a model is good for heavy read-based applications but inefficient when multiple write transactions try to modify the database concurrently.


