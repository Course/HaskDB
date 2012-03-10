{-# LANGUAGE NoMonomorphismRestriction #-}
-- | File Transactions Module .
-- Every write Transaction is first written to a journal File . Every commited journal file ensures a valid transaction . A queue of bloomFilter of the  changed blocks per journal file is maintained . A pure read Transaction never blocks . When a transaction finishes it checks the file version when the transaction started and when it finishes are the same. If not then it checks whether the blocks read and the blocks changed since it started are disjoint or a higher priority transaction is not going to fail if this transaction succeeds. If any of the above happens then transaction succeeds and commits the journal file and adds the newly calculted bloomfilter to the queue. This operation is ensured to be atomic .  
-- If any of the above is not ensured then transaction is said to be failed . It is added to the failedQueue . A low priority process can not cause failure of a very long and high priority process . This ensures prevention of starvation . 
--
module System.HaskDB.Transactions where 

import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified System.HaskDB.FileHandling as FH 
import System.HaskDB.FileHeader 
import Data.Maybe 
import System.HaskDB.Journal 
import System.HaskDB.TransactionFH
import qualified Data.BloomFilter as BF
import qualified Data.Dequeue as DQ 
import Data.BloomFilter.Hash (cheapHashes)
import Data.IORef
import Data.Unique


-- | DataType for the Transaction.
data FT a = 
    Done a |
    ReadBlock BlockNumber (BS.ByteString -> FT a) |
    WriteBlock BlockNumber BS.ByteString (FT a)

-- | Monad Definition for the Transaction . Everything happens inside this monad . 
instance Monad FT where 
    return = Done 
    m >>= f = case m of 
        Done a -> f a 
        ReadBlock bn c -> ReadBlock bn (\i -> c i  >>= f) 
        WriteBlock bn x c -> WriteBlock bn x (c >>= f)

-- PANKAJ implement below 2 functions in the TransactioFH and delete from here . 
-- Check  Failure should also check the failure queue for priority and failure . 
checkFailure :: FileVersion -> FileVersion -> TFile -> [BlockNumber] -> IO Bool 
checkFailure = undefined 

commitJournal :: Journal -> IO ()
commitJournal = undefined 

commit :: FileVersion -> (a,Transaction) -> TFile -> IO (Maybe a) 
commit  oldFileVersion (output,trans) fh = do 
    _ <- takeMVar (FH.synchVar $ fHandle fh)
    newFileVersion <- getFileVersion $ fHandle fh  
    cf <- checkFailure oldFileVersion newFileVersion fh (rBlocks trans) 
    if cf 
        then do 
        -- PANKAJ check your concept of back or front .. 
            case bloom trans of 
                Just bl -> do 
                    let jr = fromJust $ journal trans 
                    -- Locking to ensure atomicity of commit and pushing it to the queue .
                    -- Adds the current transaction written block bloom filter to the queue . Back is the  latest entry .  
                    atomicModifyIORef (jQueue fh) (\q -> (DQ.pushBack q $ JInfo jr bl,()))
                    -- No interleaving here ensured by the MVar locking . 
                    commitJournal jr
                _ -> return ()
            putMVar (FH.synchVar $ fHandle fh) () 
            return $ Just output 
        else do 
            putMVar (FH.synchVar $ fHandle fh) () 
            return Nothing 

    


data Transaction = Transaction {
    journal :: Maybe Journal ,
    bloom :: Maybe (BF.Bloom BlockNumber) ,
    rBlocks :: [BlockNumber]
    }



runTransaction :: FT a -> TFile -> IO a
runTransaction = runRetryTransaction Nothing 

-- | Maybe field is Not nothing if transaction has failed once . 
runRetryTransaction :: Maybe Unique -> FT a -> TFile -> IO a
runRetryTransaction  failure ft fh = do 
    fileVersion <- getFileVersion $ fHandle fh 
    tid <- case failure of 
            Nothing  -> newUnique 
            Just a -> return a 
    out <- trans ft fh $ Transaction Nothing Nothing [] 
    cm <- commit fileVersion out fh 
    case cm of 
        Nothing -> do 
            -- Experiment with the hashfunction and number of bits 
            if isNothing failure then 
                -- Adds the  current transaction readBlocks bloom filter to the failed transaction queue .
                atomicModifyIORef (failedQueue fh) $ \q -> (DQ.pushBack q $ (tid,BF.fromListB (cheapHashes 20) 4096 (rBlocks.snd $ out)),())
            else  
                return ()
            -- Transaction failed once so Bool field is set to  true . This is done to  prevent repetitive addition of the transaction to the failure queue. 
            runRetryTransaction (Just tid) ft fh 
        Just a -> 
            -- Previously failed transaction succeeds now . So delete it from the failure queue . 
            if not $ isNothing failure 
                then do 
                        atomicModifyIORef (failedQueue fh) $ \q -> (deleteFromQueue q tid,())
                        return a  
                else 
                    return a  

  where 
    deleteFromQueue :: DQ.BankersDequeue (Unique,JBloom) -> Unique -> DQ.BankersDequeue (Unique,JBloom)
    deleteFromQueue q id = do 
        let (a,newQ) = DQ.popFront q 
        case a of 
            Nothing -> newQ 
            Just e@(qid,bl) -> if qid /= id then DQ.pushFront (deleteFromQueue newQ id) e else newQ
    trans :: FT a -> TFile -> Transaction -> IO (a,Transaction)
    trans (Done a) _ d = return (a,d) 
    trans (ReadBlock bn c) fh d = do 
        val <- readBlock (fHandle fh) bn 
        trans (c val) fh $ d {rBlocks = bn : rBlocks d}
    trans (WriteBlock bn x c) fh d = do 
        des <- case journal d of 
                    Nothing -> newJournal $ fHandle fh 
                    Just oldDes -> return oldDes
        let bl = case bloom d of 
                    -- Experiment with the hashfunction and number of bits 
                    Nothing -> BF.emptyB (cheapHashes 20) 4096
                    Just oldBl -> oldBl
        let newBl = BF.insertB bn bl  
        writeToJournal des bn x   -- To be ultimately written to the database by the sequencer 
        trans c fh (d {journal = Just des,bloom = Just newBl} )

