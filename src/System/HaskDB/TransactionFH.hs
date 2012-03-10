module System.HaskDB.TransactionFH where 

import qualified Data.ByteString as BS
import qualified System.HaskDB.FileHandling as FH 
import qualified Data.Dequeue as DQ 
import qualified System.HaskDB.Journal as JU
import Data.Maybe
import Data.Dequeue
import Data.BloomFilter.Easy
import Data.IORef 
import qualified Data.BloomFilter as BF 

type BlockNumber = Integer
type JBloom = BF.Bloom BlockNumber
data JInfo = JInfo 
    { getJournal :: JU.Journal
    , getBloomFilter :: JBloom
    }

data TFile = TFile {
    handle :: FH.FHandle ,
    jQueue  :: IORef (DQ.BankersDequeue JInfo)
    }


checkInBloomFilter bf bn = elemB bn bf

-- | Read a block from the most recent journal that contains it 
-- else read it from the database
readBlockJ :: TFile -> BlockNumber -> IO BS.ByteString
readBlockJ tf bn = do
        q <- readIORef $ jQueue tf
        func q bn
    where 
        func q bn = case popFront q of
                    (Just jInfo, q') ->  
                        case checkInBloomFilter (getBloomFilter jInfo) bn of
                            False -> func q' bn
                            True -> do
                                d <- JU.readFromJournal (getJournal jInfo) bn
                                case d of
                                    Just x -> return x
                                    Nothing -> func q' bn
                    (Nothing , _) -> FH.readBlock (handle tf) bn -- read from database file


