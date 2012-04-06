## Abstract 

Transactions form an integral part of any multi-user concurrent database system. 
Most databases implement transactions by various locking mechanism . We present 
here lock free composable file transaction implemented in a high level 
functional programming language , Haskell. The use of haskell allows  us to easily 
compose transactions and run them as a single atomic transaction thus providing 
a powerful abstraction for the programmer's use . The implementantion is not 
only highly concurrent but also non blocking as there are no locks on files. 
We have cleverly exploited the space efficient probabilistic data structure 
called Bloom Filters to keep primary memory requirements as low as possible.


