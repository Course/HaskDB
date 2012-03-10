{-# LANGUAGE NoMonomorphismRestriction #-}
module System.HaskDB.Transactions where 

import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified System.HaskDB.FileHandling as FH 
import Data.Maybe 
import System.HaskDB.Journal 
import System.HaskDB.TransactionFH
import qualified Data.BloomFilter as BF
import qualified Data.Dequeue as DQ 
import Data.BloomFilter.Hash (cheapHashes)
import Data.IORef

-- | Some definitions to change the datatype afterwards . 
data BlockData = BlockData BS.ByteString 
data LogDescriptor = LogDescriptor 
type FileInformation = FH.FHandle 
type FileVersion = BS.ByteString

readBlock = FH.readBlock
writeBlock = FH.writeBlock

readBlockJ :: TFile -> BlockNumber -> IO a 
readBlockJ = undefined  -- To be changed according to file handling api 
writeBlockJ :: TFile -> BlockNumber -> a -> IO a 
writeBlockJ = undefined  -- To be changed according to file handling api 


-- | Reads the current file version from the disk . 
getFileVersion :: FileInformation -> IO FileVersion 
getFileVersion = undefined 

-- | Used to change the current file version . This actually writes to a journal file for it to persist even in case of system crash 
changeFileVersion :: FileInformation -> FileVersion -> IO ()
changeFileVersion = undefined 

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
checkFailure :: FileVersion -> FileVersion -> TFile -> [BlockNumber] -> Bool 
checkFailure = undefined 

commitJournal :: Journal -> IO ()
commitJournal = undefined 

commit :: FileVersion -> (a,Transaction) -> TFile -> IO (Maybe a) 
commit  oldFileVersion (output,trans) fh = do 
    newFileVersion <- getFileVersion $ fHandle fh  
    if checkFailure oldFileVersion newFileVersion fh (rBlocks trans) 
        then do 
        -- PANKAJ check your concept of back or front .. 
            _ <- takeMVar (FH.synchVar $ fHandle fh)
            case bloom trans of 
                Just bl -> do 
                    let jr = fromJust $ journal trans 
                    atomicModifyIORef (jQueue fh) (\q -> (DQ.pushBack q $ JInfo jr bl,()))
                    commitJournal jr
                _ -> return ()
            putMVar (FH.synchVar $ fHandle fh) () 
            return $ Just output 
        else return Nothing 

    


data Transaction = Transaction {
    journal :: Maybe Journal ,
    bloom :: Maybe (BF.Bloom BlockNumber) ,
    rBlocks :: [BlockNumber]
    }


runTransaction :: FT a -> TFile -> IO a
runTransaction ft fh = do 
    fileVersion <- getFileVersion $ fHandle fh 
    out <- trans ft fh $ Transaction Nothing Nothing [] 
    cm <- commit fileVersion out fh 
    case cm of 
        Nothing -> do 
            -- Experiment with the hashfunction and number of bits 
            atomicModifyIORef (failedQueue fh) $ \q -> (DQ.pushBack q $ JInfo (fromJust.journal.snd $ out) (BF.fromListB (cheapHashes 20) 4096 (rBlocks.snd $ out)),())
            -- Not corrent . Change later 
            runTransaction ft fh 
        Just a -> return a  

  where 
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

