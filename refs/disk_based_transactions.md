% Concurrent Disk Based Transactions in Haskell
% Satvik Chauhan and Pankaj More 
% April 16 , 2012


# Shared Resource Problem 

* **Problem** : Share a resource between multiple concurrent threads.
* Results in several problems.
* Lost update 
<pre style="border:none;font-size:60%;background-color:#fff">
+----------P1-------------+-----+------------P2------------+
|                         +  1  + w <- read(A)             |
| u <- read(A)            +  2  +                          |
| write(A,u+100)          +  3  +                          |
|                         +  4  + write(A,w+100)           |
+-------------------------+-----+--------------------------+
</pre>
* Incorrect summary problem 
<pre style="border:none;font-size:60%;background-color:#fff">
+----------P1-------------+-----+------------P2------------+
|                         +  1  + w <- read(A)             |
|                         +  2  + write(A,w-100)           |
| u1 <- read(A)           +  3  +                          |
| u2 <- read(B)           +  4  +                          |
| sum = a + b             +  5  +                          |
|                         +  6  + x <- read(B)             |
|                         +  7  + write(B,x+100)           |
+-------------------------+-----+--------------------------+
</pre>
* Many more problems like the above.

# Shared Resource Problem (Cont)

* **Solution** : Locks !!
* Problems with Locks
    * Race Conditions if locks are forgotten. 
    * Deadlocks from inconsistent locks ordering. 
    * Uncaught exceptions might result in any of the above problems. 
    * Coarse Locks hurt performance.
    * Locks don't compose.

# Shared Resource Problem (Cont)

* Two phase locking 
    * Any transaction first must acquire locks for all the shared resources.
    * Perform the operations on the shared resources.
    * Release all the locks.
* Still deadlock problem. 
<pre style="border:none;font-size:60%;background-color:#fff">
+----------P1-------------+-----+------------P2------------+
|                         +  1  + Acquire(A)               |
| Acquire(B)              +  2  +                          |
|                         +  3  + Acquire(B)               |
| Acquire(A)              +  4  +                          |
| Some operations         +  5  +                          |
| Release(B)              +  6  +                          |
| Release(A)              +  7  +                          |
|                         +  8  + Some operations          |
|                         +  9  + Release(A)               |
|                         +  10 + Release(B)               |
+-------------------------+-----+--------------------------+
</pre>

* Can prevent deadlocks by acquiring locks in certain fixed order.
* Still hurts performance


# Transactions 

* An optimistic way of managing shared resources.
* A set of operations which are performed on a resource either fully or none at all. 
* Helps to maintain a consistent view of the resource. 
(TRANSACTION DIAGRAM HERE)

# Transactions (Cont)

* **Atomicity** 
    * Transaction either commits or fails 
* **Consistency**
    * Resource is in a consistent state after a transaction commits. 
* **Isolation** 
    * Unaware of the affects of concurrently running transactions. 
* **Durability**
    * Once a transaction commits , changes should persist. 

# Transactions in Databases (Atleast 5 slides)

* serializabilty

* In a serial schedule, each transaction is performed in its entirety in serial 
  order. There is no interleaving.

* DEFINITION: A schedule S is said to be serial if, for every transaction T 
participating in the schedule, all the operations of T are executed 
consecutively in the schedule; otherwise, the schedule is called nonserial.

# Transactions in Databases (Atleast 5 slides)

* Why we like Interleafing

* In a serial schedule, if a transaction waits for an I/O operation to complete, 
idle CPU time is generated and wasted for lack of use.

* Other transactions may also be in line waiting for the completion of a 
transaction.

* For these reasons, serial schedules are generally considered unacceptable in 
practice.

* Interleaving could improve the use of the CPU cycles.

# Transactions in Databases (Atleast 5 slides)

# Transactions in Databases (Atleast 5 slides)

# Transactions in Databases (Atleast 5 slides)

# Software Transactional Memory (STM)

* New way of programming on multicore systems. 
* Optimistic way of running transactions. 
* Allows all transactions to run simultaneously.
    * Transactions perform changes to their own local buffer. 
    * At the time of commit, decide success or failure. 
    * Success : The changes become simultaneously visible to other threads. 
    * Failure : No changes made to the memory. 
* Compose well. 

# Why Haskell?

* Haskell's expressive power can improve productivity
    * Small language core provides big flexibility
    * Code can be very concise, speeding development
    * Get best of both worlds from compiled and interpreted languages

* Haskell makes code easier to understand and maintain
    * Can dive into complex libraries and understand *what* the code
      is doing

* Haskell can increase the robustness of systems
    * Strong typing catches many bugs at compile time
    * Functional code permits better testing methodologies
    * Can parallelize non-concurrent code without changing semantics
    * Concurrent programming abstractions resistant to data races

* Lots of compiler Optimizations 
    * Fusion, inlining etc

# Haskell is a *pure* functional language

* Unlike variables in imperative languages, Haskell bindings are
    * *immutable* - can only bind a symbol once in a give scope<br>
      (We still call bound symbols "variables" though)

    ~~~ {.haskell}
    x = 5
    x = 6                      -- error, cannot re-bind x
    ~~~

    * *order-independent* - order of bindings in source code does not
       matter
    * *lazy* - definitions of symbols are evaluated only when needed

    ~~~ {.haskell}
    evens = map (*2) [1..] -- infinite list of even numbers
    main = print (take 50 evens) -- prints first 50 even numbers
    ~~~

    * *recursive* 
    
# Haskell , Transactions and STM 

* Transactions Using Locks 

~~~ {.haskell}
transfer :: Account -> Account -> Int -> IO ()
transfer to from amount = do 
    acquire fromLock
    acquire toLock 
    withdraw from amount 
    deposit to amount 
    release fromLock 
    release toLock
~~~

* Easy to result into deadlocks.

# Haskell , Transactions and STM (Cont)

* STM  

~~~ {.haskell}
withdraw :: Account -> Int -> STM ()
withdraw acc amount = do 
    bal <- readTVar acc
    writeTVar acc (bal - amount) 

deposit :: Account -> Int -> STM ()
deposit acc amount = withdraw acc (-amount)

transfer :: Account -> Account -> Int -> IO ()
transfer to from amount = atomically (do 
    withdraw from amount 
    deposit to amount)

~~~

* No deadlocks 
* Easy to compose 
* Type system prevents any IO operations to be performed inside a transaction.



        return = Just
        fail _ = Nothing
    ~~~~

* You can use `Nothing` to indicate failure
    * Might have a bunch of functions to extract fields from data

    ~~~~ {.haskell}
    extractA :: String -> Maybe Int
    extractB :: String -> Maybe String
    ...
    parseForm :: String -> Maybe Form
    parseForm raw = do
        a <- extractA raw
        b <- extractB raw
        ...
        return (Form a b ...)
    ~~~~

    * Threads success/failure state through system as `IO` threaded
      World
    * Since Haskell is lazy, stops computing at first `Nothing`

# Disk Based Transactions (Atleast 5 pages)

# Disk Based Transactions (Atleast 5 pages)

# Disk Based Transactions (Atleast 5 pages)

# Disk Based Transactions (Atleast 5 pages)

# Disk Based Transactions (Atleast 3-4 pages)

# Implementations (Atleast  4 pages)

# Implementations (Atleast  4 pages)

# Implementations (Atleast  4 pages)

# Implementations (Atleast  4 pages)

