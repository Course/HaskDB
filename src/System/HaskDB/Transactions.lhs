> {-# LANGUAGE NoMonomorphismRestriction #-}

> -- | File Transactions Module .
> --  Every write Transaction is first written to a journal File . Every commited journal file ensures a valid transaction . 
> --  A queue of bloomFilter of the  changed blocks per journal file is maintained . A pure read Transaction never blocks . 
> --  When a transaction finishes it checks the file version when the transaction started and when it finishes are the same. 
> --  If not then it checks whether the blocks read and the blocks changed since it started are disjoint or a higher priority transaction is not going to fail if this transaction succeeds. 
> --  If any of the above happens then transaction succeeds and commits the journal file and adds the newly calculted bloomfilter to the queue. This operation is ensured to be atomic .  
> --  If any of the above is not ensured then transaction is said to be failed . It is added to the failedQueue . A low priority process can not cause failure of a high priority process . 
> --  This ensures prevention of starvation . The priority of a failed process increases with time . And if it very high , it will be allowed to go through resulting in failure of other 
> --  intersecting trensactions 

> module System.HaskDB.Transactions (
>    runTransaction 
>    , readBlock 
>    , writeBlock
>    ) where 

> import Control.Concurrent 
> import qualified Data.ByteString as BS 
> import qualified System.HaskDB.FileHandling as FH 
> import System.HaskDB.FileHeader 
> import Data.Maybe 
> import System.IO
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

> -- | Monad Definition for the Transaction . Everything happens inside this monad . 
> instance Monad FT where 
>     return = Done 
>     m >>= f = case m of 
>         Done a -> f a 
>         ReadBlock bn c -> ReadBlock bn (\i -> c i  >>= f) 
>         WriteBlock bn x c -> WriteBlock bn x (c >>= f)

> -- | readBlockT to be used inside the FT Monad 
> readBlockT = flip ReadBlock return 

> -- | writeBlockT to be used inside the FT Monad 
> writeBlockT v x =  WriteBlock v x $ return () 

> checkSuccess = undefined 

> data Transaction = Transaction {
>     rBlocks :: BlockList 
>     , tType :: TransactionType 
>     }

> data TransactionType = ReadOnly | ReadWrite {
>         bloom :: BF.Bloom BlockNumber
>         , journal :: Journal 
>         }

> -- | Runs the given transaction on the file .
> runTransaction :: FT a -> TFile -> IO a
> runTransaction = runRetryTransaction Nothing 

> runRetryTransaction :: Maybe Unique -> FT a -> TFile -> IO a
> runRetryTransaction  failure ft fh = do 
>     (tid,fileVersion) <- bracket_ (takeMVar (FH.synchVar $ fHandle fh)) (putMVar (FH.synchVar $ fHandle fh) ())
>         ( do 
>             fileVersion <- getFileVersion $ fHandle fh 
>             tid <- maybe (newUnique) return failure
>             atomicModifyIORef (transactions fh) $ \q -> (DQ.pushBack q $ (tid,fileVersion) , ())
>             return (tid,fileVersion))  
>     let tf = (show $ hashUnique tid) ++ ".trans"
>     FH.truncateF tf 
>     tfh <- FH.openF tf ReadWriteMode (FH.blockSize $ fHandle fh)
>     cm  <- commit fileVersion fh =<< (trans ft fh $ Transaction (BlockList [] 8 tfh) ReadOnly)
>     FH.closeF tfh
>     atomicModifyIORef (transactions fh) $ \q -> (deleteFromQueue q tid, ())
>     maybe (do 
>         atomicModifyIORef (failedQueue fh) $ \q -> (DQ.pushBack q $ (tid,fileVersion),()) 
>         runRetryTransaction (Just tid) ft fh )
>         (\a -> do 
>                 atomicModifyIORef (failedQueue fh) $ \q -> (deleteFromQueue q tid,())
>                 return a) cm
>   where 
>     deleteFromQueue :: DQ.BankersDequeue (Unique,a) -> Unique -> DQ.BankersDequeue (Unique,a)
>     deleteFromQueue q id = do 
>         let (a,newQ) = DQ.popFront q 
>         case a of 
>             Nothing -> newQ 
>             Just e@(qid,bl) -> if qid /= id then DQ.pushFront (deleteFromQueue newQ id) e else newQ
>     trans :: FT a -> TFile -> Transaction -> IO (a,Transaction)
>     trans (Done a) _ d = return (a,d) 
>     trans (ReadBlock bn c) fh d = do 
>         val <- readBlockJ fh bn 
>         rblcks <- addBlock (fromIntegral bn) (rBlocks d)
>         trans (c val) fh $ d {rBlocks = rblcks}
>     -- Experiment with the hashfunction and number of bits 
>     trans (WriteBlock bn x c) fh d = do 
>         rw <- rTorw d (BF.emptyB (cheapHashes 20) 4096) <$> newJournal (fHandle fh)
>         let newBl = BF.insertB bn (bloom $ tType rw)  
>         writeToJournal (journal $ tType rw) bn x   -- To be ultimately written to the database by the sequencer 
>         trans c fh (rw {tType = (tType rw) {bloom = newBl}} )

Can be implemented in 2 ways . 

1. Keep Restoring Journal and popping them from the Queue until you find a journal  which has a version on which we have a currently active 
 transaction running . Yield () .
2. Keep Restoring Journal without popping from the queue and also keep a copy of journals restored. Now Another thread will wait until there are not any transactions which have started on or before that 
 version of the journal and delete the journal from the queue and also the journal file . 

The Second version requires extra memory .
I am implementing the first Version here 

> sequencer :: TFile -> IO () 
> sequencer fh = do 
>     b <- readIORef $ jQueue fh
>     if  DQ.null b then yield else do
>         let j = getJournal . fromJust $ DQ.first b
>         replayJournal j 
>         popFromJournalQueue j fh 
>         sequencer fh
>     where 
>         popFromJournalQueue j fh = do 
>             tq <- readIORef $ transactions fh 
>             if checkToDelete (journalID j) tq then 
>                     atomicModifyIORef (jQueue fh) $ \q -> (snd  $ DQ.popFront q , ())
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
> commit  oldFileVersion fh (output,trans) = do 
>     bracket_ (takeMVar $ FH.synchVar $ fHandle fh) (putMVar (FH.synchVar $ fHandle fh) () )
>         ( do 
>             newFileVersion <- getFileVersion $ fHandle fh  
>             cf <- checkSuccess oldFileVersion newFileVersion fh (rBlocks trans) 
>             if  not cf then return Nothing else do 
>                     case trans of 
>                         Transaction _ (ReadWrite bl jr) -> do 
>                             atomicModifyIORef (jQueue fh) (\q -> (DQ.pushBack q $ JInfo jr bl,()))
>                             commitJournal jr newFileVersion
>                         _ -> return ()
>                     return $ Just output ) 
> 
> rTorw :: Transaction -> BF.Bloom BlockNumber -> Journal -> Transaction 
> rTorw t@(Transaction rb ReadOnly) bl jl = Transaction rb (ReadWrite bl jl)
> rTorw t _ _ = t
> tMap :: Transaction -> (BF.Bloom BlockNumber -> BF.Bloom BlockNumber) -> (Journal -> Journal) -> Transaction
> tMap t@(Transaction rb ReadOnly) _ _ = t
> tMap (Transaction rb (ReadWrite bl jl)) fbl fjl = Transaction rb (ReadWrite (fbl bl) (fjl jl))
