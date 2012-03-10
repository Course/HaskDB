{-# LANGUAGE NoMonomorphismRestriction #-}
module System.HaskDB.Transactions where 

import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified System.HaskDB.FileHandling as FH 
import Data.Maybe 
import System.HaskDB.Journal 
import qualified Data.BloomFilter as BF
import Data.BloomFilter.Hash (cheapHashes)

-- | Some definitions to change the datatype afterwards . 
data TFile = TFile {
    handle :: FH.FHandle 
    }
type BlockNumber = Integer
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

checkFailure :: FileVersion -> FileVersion -> TFile -> [BlockNumber] -> Bool 
checkFailure = undefined 

data Transaction = Transaction {
    journal :: Maybe Journal ,
    bloom :: Maybe (BF.Bloom BlockNumber) ,
    rBlocks :: [BlockNumber]
    }


runTransaction :: FT a -> TFile -> IO (Maybe a) 
runTransaction ft fh = do 
    fileVersion <- getFileVersion $ handle fh 
    (output ,trans) <- trans ft fh $ Transaction Nothing Nothing [] 
    if isNothing $ journal trans  then do 
        -- Only read  transaction 
        newFileVersion <- getFileVersion $ handle fh  
        if checkFailure fileVersion newFileVersion fh (rBlocks trans) then 
            return $ Just output 
            else return Nothing 
        else do   
                -- read Write Transaction
                return $ Just output

  where 
    trans :: FT a -> TFile -> Transaction -> IO (a,Transaction)
    trans (Done a) _ d = return (a,d) 
    trans (ReadBlock bn c) fh d = do 
        val <- readBlock (handle fh) bn 
        trans (c val) fh $ d {rBlocks = bn : rBlocks d}
    trans (WriteBlock bn x c) fh d = do 
        des <- case journal d of 
                    Nothing -> newJournal $ handle fh 
                    Just oldDes -> return oldDes
        let bl = case bloom d of 
                    -- Experiment with the hashfunction and number of bits 
                    Nothing -> BF.emptyB (cheapHashes 20) 4096
                    Just oldBl -> oldBl
        let newBl = BF.insertB bn bl  
        writeToJournal des bn x   -- To be ultimately written to the database by the sequencer 
        trans c fh (d {journal = Just des,bloom = Just newBl} )

