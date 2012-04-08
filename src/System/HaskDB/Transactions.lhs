> {-# LANGUAGE NoMonomorphismRestriction #-}

> -- | File Transactions Module .
> module System.HaskDB.Transactions (
>    runTransaction 
>    , retryTransaction
>    , readBlockT 
>    , writeBlockT
>    , openTF 
>    , closeTF 
>    , sequencer
>    ) where 
> import Control.Concurrent 
> import qualified Data.ByteString as BS 
> import qualified System.HaskDB.FileHandling as FH 
> import System.HaskDB.FileHeader 
> import Data.Maybe 
> import System.IO hiding (withFile)
> import System.HaskDB.Journal 
> import System.HaskDB.TransactionFH hiding(checkFailure)
> import qualified Data.BloomFilter as BF
> import qualified Data.Dequeue as DQ 
> import Data.BloomFilter.Hash (cheapHashes)
> import Data.IORef
> import Data.Unique
> import Control.Applicative
> import Control.Exception

The main task is to capture the transaction into a datatype first. Here we are implementing a very basic version of transactions. So our transaction system only provide two type of operations to be performed on the file. 

* **ReadBlock** is to read the data from the given block number. 
* **WriteBlock** is to write the given data on the block number provide. 

We can also think of adding operations like append block , modify block etc. , but to keep it simple we only support these two basic operations. 
Now lets look at the data definition of the File Transaction (FT) data type . 

> -- | Transaction DataType 
> data FT a = 
>     Done a | -- ^ Any value in the FT monad will be captured in Done a. 
>     ReadBlock BlockNumber (BS.ByteString -> FT a) | -- ^ FT a here represents
> -- rest of the computation. This follows from the continuation passing style.
>     WriteBlock BlockNumber BS.ByteString (FT a) -- ^ FT a here is similar to 
> -- above to have continuations. 

Dont be scared from the types of ReadBlock and WriteBlock. We will see later how it helps in actually passing the continuations.
Lets see the monad definiton of the FT datatype.

> -- | Monad Definition for the Transaction. 
> instance Monad FT where 
>     return = Done 
>     m >>= f = case m of 
>         Done a -> f a 
>         ReadBlock bn c -> ReadBlock bn (\i -> c i  >>= f) 
>         WriteBlock bn x c -> WriteBlock bn x (c >>= f)

Here we will see how the types of ReadBlock and WriteBlock actually help in our continuation passing style of programming. Lets see a simple example of interface our implementation provides . 
Consider the famous banking example for transactions. We want to transfer x fund from account A to account B.
Lets assume that fund informations of A and B are stored in the same file at block number a and b respectively.
Here is a function which deposits x amount to the given account. 

> deposit a x = do 
>       block <- ReadBlock a return 
>       WriteBlock a (increase block x) (return ())
>   where 
>        increase bs x = undefined -- amount bs = amount bs + x 

Lets see how this is translated to explicit notation without do notation. 

ReadBlock a (\block -> return block >>= (\block -> WriteBlock a (increase block x) (return () )))
= ReadBlock a (\block -> Done block  >>= (\block -> WriteBlock a (increase block x) (return () )))
= ReadBlock a (\block -> WriteBlock a (increase block x) (return ()) )
Looks like it got transformed to whatever we wanted. 
It was a liitle frustrating to write return while writing ReadBlock and WriteBlock. So lets define feh helpers to help us avoiding the repetitions. 

> -- | readBlockT to be used inside the FT Monad 
> readBlockT :: BlockNumber -> FT BS.ByteString
> readBlockT = flip ReadBlock return 
> -- | writeBlockT to be used inside the FT Monad 
> writeBlockT :: BlockNumber -> BS.ByteString -> FT ()
> writeBlockT v x =  WriteBlock v x $ return () 

Now we want to actually perform the transactions satisfying all the ACID guarantees. So we need to write a fucntion to actually convert our Transactions from FT monad to IO monad and perform them. 
According to the semantics of a transaction , a transaction can either fail or succeed. So we should provide atleast two types of functions to run a transaction which are as follows :

> -- | Runs the given transaction on the file. 
> -- Transaction may fail in which case it returns Nothing.
> runTransaction :: FT a  -- ^ FileTransaction to be performed 
>                -> TFile -- ^ File on which this transaction is to be performed
>                -> IO (Maybe a)
> runTransaction = runT Nothing False
> 
> -- | Runs the transaction on the file. 
> -- If transaction fails then repeats it with higher priority.
> retryTransaction :: FT a  -- ^ FileTransaction to be performed 
>                  -> TFile -- ^ File on which transaction is performed 
>                  -> IO a
> retryTransaction ft tFile = fromJust <$> runT Nothing True ft tFile 

At this point before implementing anything else we are interested in how we will be actiually using them. Here I will also introduce you to the power of composing two transactions and running them as one. Lets comeback to our backing example. 

> transfer a b x = do 
>   deposit a (-x) 
>   deposit b x 

Here is the function to remove x amount from account A and deposit it to the account B. We have implemented a very loose semantics here as to not check if A's balance is less than 0 etc.  I just wanted to show the power of composing functions. Now we can just do runTransaction on transfer to run this transaction. The semantics of runTransaction automatically takes care of all the possible failures and rollback in case of transaction failure. 
Now comes the core function of our implementation which actually perform all the actions. 

> runT :: Maybe Integer -- Nothing if transaction never failed else Just id 
>      -> Bool  -- True if the transaction is to be retried in case of failure
>      -> FT a  -- Transaction 
>      -> TFile -- The File on which to run this transaction 
>      -> IO (Maybe a)  
> 
> runT  failure retry ft tFile = do 
>     -- Add the transaction to the transaction queue and get the current 
>     -- fileversion. This has to be removed after commit is performed. 
>     (tid,fileVersion) <- withSynch (FH.synchVar $ fHandle tFile) 
>                                    (addToTransactionQ tFile)
>     let transFile = newTransactionFile tid
>     -- Performs the transaction on the journal file and commits. If commit 
>     -- succeeded then return Just output else Nothing 
>     maybeOut <- withFile transFile $ runAndCommit fileVersion tFile ft 
>     atomicModifyIORef (transactions tFile) $ \q -> (deleteFromQueue q tid, ())
>     if retry then retryIfFailed failure maybeOut fileVersion tid ft tFile 
>         else return maybeOut
>   where 
>     retryIfFailed :: Maybe Integer -> Maybe a -> FileVersion -> Integer
>                   -> FT a -> TFile -> IO (Maybe a) 
>     retryIfFailed Nothing out@(Just a) _ _ _ _ = return out 
>     retryIfFailed (Just tid) out@(Just a) _ _ _ tFile = do   
>         atomicModifyIORef (failedQueue tFile) 
>                           (\q -> (deleteFromQueue q tid,()))
>         return out 
>     retryIfFailed Nothing Nothing fileVersion tid ft tFile = do 
>         atomicModifyIORef (failedQueue tFile) 
>                           (\q -> (DQ.pushBack q $ (tid,fileVersion),()))
>         runT (Just tid) True ft tFile 
>     retryIfFailed (Just tid) Nothing fileVersion _ ft tFile =  
>         runT (Just tid) True ft tFile 
>
>     runAndCommit :: FileVersion -> TFile -> FT a -> FH.FHandle -> IO (Maybe a)
>     runAndCommit fv tFile ft fh = do 
>         out <- trans ft tFile $ Transaction (BlockList [] 8 fh) ReadOnly
>         commit fv tFile out 
>
>     newTransactionFile :: Integer -> String
>     newTransactionFile tid = (show tid) ++ ".trans"
>
>     addToTransactionQ :: TFile -> IO (Integer,FileVersion)
>     addToTransactionQ fh = do 
>         fileVersion <- getFileVersion $ fHandle fh 
>         tid <- maybe (atomicModifyIORef (FH.journalId $ fHandle fh) (\a-> (a+1,a))) return failure
>         atomicModifyIORef (transactions fh) 
>                           (\q -> (DQ.pushBack q $ (tid,fileVersion) , ()))
>         return (tid,fileVersion) 
>
>     deleteFromQueue :: DQ.BankersDequeue (Integer,a) -> Integer
>                     -> DQ.BankersDequeue (Integer,a)
>     deleteFromQueue q id = do 
>         let (a,newQ) = DQ.popFront q 
>         case a of 
>             Nothing -> newQ 
>             Just e@(qid,bl) -> if qid /= id 
>                 then DQ.pushFront (deleteFromQueue newQ id) e 
>                else newQ
>
>     trans :: FT a -> TFile -> Transaction -> IO (a,Transaction)
>     trans (Done a) _ d = do 
>         return (a,d) 
>     trans (ReadBlock bn c) fh d = do 
>         val <- readBlockJ fh bn d
>         rblcks <- addBlock (fromIntegral bn) (rBlocks d)
>         trans (c val) fh $ d {rBlocks = rblcks}
>     -- Experiment with the hashfunction and number of bits.  
>     trans (WriteBlock bn x c) fh d = do 
>         rw <- rToRw d (BF.emptyB (cheapHashes 20) 4096) (newJournal (fHandle fh))
>         let newBl = BF.insertB bn (bloom $ tType rw)  
>         j <- writeToJournal (journal $ tType rw) bn x
>         trans c fh (rw {tType = (tType rw) {bloom = newBl,journal = j}} )

All the journals of the transactions get aggregated over time which might result in poor read performances over time. So we need to actually checkpoint the commited journals back to the database file. 

> sequencer :: TFile -> IO () 
> sequencer fh = do 
>     b <- readIORef $ jQueue fh
>     if  DQ.null b then yield else do
>         let j = getJournal . fromJust $ DQ.first b
>         replayJournal j 
>         FH.flushBuffer (fHandle fh)
>         popFromJournalQueue j fh 
>         sequencer fh
>     where 
>         popFromJournalQueue j fh = do 
>             tq <- readIORef $ transactions fh 
>             if checkToDelete (journalID j) tq then 
>                     atomicModifyIORef (jQueue fh) 
>                                      (\q -> (snd  $ DQ.popFront q , ()))
>                 else do  
>                     yield
>                     popFromJournalQueue j fh
>         checkToDelete jid tq = do 
>             let (front , _) = DQ.popFront tq 
>             case front of 
>                 Nothing -> True 
>                 Just (id,fv) -> fv >= jid 
> 
> commit :: FileVersion -> TFile -> (a,Transaction) -> IO (Maybe a) 
> commit  oldFV fh (output,trans) = do 
>     -- synch is used to prevent 2 commits from interleaving 
>     out <- withSynch (commitSynch fh)
>         (do 
>             newFV <- getFileVersion $ fHandle fh  
>             print $ "Old fv " ++ (show oldFV) 
>             cf <- checkSuccess oldFV newFV fh (rBlocks trans) 
>             if  not cf then return Nothing else do 
>                 case trans of 
>                     Transaction _ (ReadWrite bl jr) -> do 
>                         atomicModifyIORef (jQueue fh) 
>                                       (\q -> (DQ.pushBack q $ JInfo jr bl (newFV + 1),()))
>                         commitJournal jr
>                     _ -> return ()
>                 return $ Just output ) 
>     case trans of 
>         Transaction _ t@(ReadWrite _ jl ) -> do FH.flushBuffer $ jHandle jl
>                                                 FH.flushBuffer $ hHandle jl
>         otherwise -> return ()
>     return out 
>
> withSynch :: MVar () -> IO b -> IO b
> withSynch synchVar  = bracket_ (takeMVar synchVar) (putMVar synchVar ())
> withFile :: FilePath -> (FH.FHandle -> IO a) -> IO a 
> -- Blocksize taken to be 4096 Bytes 
> withFile fp = bracket (FH.openF fp ReadWriteMode 4096) FH.closeF
>                   
> rToRw :: Transaction -> BF.Bloom BlockNumber -> IO Journal -> IO Transaction 
> rToRw t@(Transaction rb ReadOnly) bl jl = do 
>     j <- jl 
>     return $ Transaction rb (ReadWrite bl j)
> rToRw t _ _ = return t 
> tMap :: Transaction -> (BF.Bloom BlockNumber -> BF.Bloom BlockNumber) 
>      -> (Journal -> Journal) -> Transaction
> tMap t@(Transaction rb ReadOnly) _ _ = t
> tMap (Transaction rb (ReadWrite bl jl)) fbl fjl = 
>                                 Transaction rb (ReadWrite (fbl bl) (fjl jl))
