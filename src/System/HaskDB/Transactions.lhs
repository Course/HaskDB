> {-# LANGUAGE NoMonomorphismRestriction #-}

> -- | File Transactions Module .
> --  Every write Transaction is first written to a journal File. Every commited
> --  journal file ensures a valid transaction. A queue of bloomFilter of the 
> --  changed blocks per journal file is maintained. A pure read Transaction 
> --  never blocks. When a transaction finishes it checks the file version when 
> --  the transaction started and when it finishes are the same. If not then it 
> --  checks whether the blocks read and the blocks changed since it started are
> --  disjoint or a higher priority transaction is not going to fail if this 
> --  transaction succeeds. If any of the above happen then transaction succeeds
> --  and commits the journal file and adds the newly calculted bloomfilter to 
> --  the queue. This operation is ensured to be atomic. If any of the above is 
> --  not ensured then transaction is said to be failed . It is added to the 
> --  failedQueue. The failed queue is used to prevent the starvation. The 
> --  priority of a failed process increases with time which with a heuristic 
> --  is used to decide when a process commit succeeds . 
> module System.HaskDB.Transactions (
>    runTransaction 
>    , retryTransaction
>    , readBlockT 
>    , writeBlockT
>    , openTF 
>    , closeTF 
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

Every Transaction is represented as the following datatype . 

> -- | Transaction DataType 
> data FT a = 
>     Done a |
>     ReadBlock BlockNumber (BS.ByteString -> FT a) |
>     WriteBlock BlockNumber BS.ByteString (FT a)

> -- | Monad Definition for the Transaction. 
> instance Monad FT where 
>     return = Done 
>     m >>= f = case m of 
>         Done a -> f a 
>         ReadBlock bn c -> ReadBlock bn (\i -> c i  >>= f) 
>         WriteBlock bn x c -> WriteBlock bn x (c >>= f)

> -- | readBlockT to be used inside the FT Monad 
> readBlockT :: BlockNumber -> FT BS.ByteString
> readBlockT = flip ReadBlock return 

> -- | writeBlockT to be used inside the FT Monad 
> writeBlockT :: BlockNumber -> BS.ByteString -> FT ()
> writeBlockT v x =  WriteBlock v x $ return () 

> data Transaction = Transaction {
>     rBlocks :: BlockList 
>     , tType :: TransactionType 
>     }

> data TransactionType = ReadOnly | ReadWrite {
>         bloom :: BF.Bloom BlockNumber
>         , journal :: Journal 
>         }

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

> runT :: Maybe Unique -- Nothing if transaction never failed else Just id 
>      -> Bool  -- True if the transaction is to be retried in case of failure 
>      -> FT a  -- Transaction 
>      -> TFile -- The File on which to run this transaction 
>      -> IO (Maybe a)  
> 
> runT  failure retry ft tFile = do 
>     (tid,fileVersion) <- withSynch (FH.synchVar $ fHandle tFile) 
>                                    (addToTransactionQ tFile)
>     transFile <- newTransactionFile tid
>     maybeOut <- withFile transFile $ runAndCommit fileVersion tFile ft 
>     atomicModifyIORef (transactions tFile) $ \q -> (deleteFromQueue q tid, ())
>     if retry then retryIfFailed failure maybeOut fileVersion tid ft tFile 
>         else return maybeOut
>   where 
>     retryIfFailed :: Maybe Unique -> Maybe a -> FileVersion -> Unique
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
>     newTransactionFile :: Unique -> IO String -- Uniqueness not guaranteed 
>     newTransactionFile tid = do  
>         let tf = (show $ hashUnique tid) ++ ".trans"
>         return tf
>
>     addToTransactionQ :: TFile -> IO (Unique,FileVersion)
>     addToTransactionQ fh = do 
>         fileVersion <- getFileVersion $ fHandle fh 
>         tid <- maybe newUnique return failure
>         atomicModifyIORef (transactions fh) 
>                           (\q -> (DQ.pushBack q $ (tid,fileVersion) , ()))
>         return (tid,fileVersion) 
>
>     deleteFromQueue :: DQ.BankersDequeue (Unique,a) -> Unique 
>                     -> DQ.BankersDequeue (Unique,a)
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
>         val <- readBlockJ fh bn 
>         rblcks <- addBlock (fromIntegral bn) (rBlocks d)
>         trans (c val) fh $ d {rBlocks = rblcks}
>     -- Experiment with the hashfunction and number of bits 
>     trans (WriteBlock bn x c) fh d = do 
>         rw <- rToRw d (BF.emptyB (cheapHashes 20) 4096) (newJournal (fHandle fh))
>         let newBl = BF.insertB bn (bloom $ tType rw)  
>         j <- writeToJournal (journal $ tType rw) bn x
>         trans c fh (rw {tType = (tType rw) {bloom = newBl,journal = j}} )

Can be implemented in 2 ways . 

1. Keep Restoring Journal and popping them from the Queue until you find a
   journal  which has a version on which we have a currently active 
 transaction running . Yield () .
2. Keep Restoring Journal without popping from the queue and also keep a copy 
   of journals restored. Now Another thread will wait until there are not any
   transactions which have started on or before that version of the journal and
   delete the journal from the queue and also the journal file . 
The Second version requires extra memory .
I am implementing the first Version here 

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
>             cf <- checkSuccess oldFV newFV fh (rBlocks trans) 
>             if  not cf then return Nothing else do 
>                 case trans of 
>                     Transaction _ (ReadWrite bl jr) -> do 
>                         atomicModifyIORef (jQueue fh) 
>                                       (\q -> (DQ.pushBack q $ JInfo jr bl,()))
>                         commitJournal jr
>                     _ -> return ()
>                 return $ Just output ) 
>     FH.flushBuffer $ jHandle $ journal $ tType trans
>     FH.flushBuffer $ hHandle $ journal $ tType trans
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
