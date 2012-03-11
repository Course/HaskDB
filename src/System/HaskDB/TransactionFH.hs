module System.HaskDB.TransactionFH where 

import qualified Data.ByteString as BS
import qualified System.HaskDB.FileHandling as FH 
import qualified Data.Dequeue as DQ 
import Data.Maybe
import Data.Dequeue
import Data.BloomFilter.Easy
import Data.IORef 
import qualified Data.BloomFilter as BF 
import System.HaskDB.Journal as JU
import qualified Data.ByteString as BS 
import Data.Unique 

data BlockData = BlockData BS.ByteString 
type BlockNumber = Integer
data LogDescriptor = LogDescriptor 
type FileInformation = FH.FHandle 
type FileVersion = Integer

readBlock = FH.readBlock
writeBlock = FH.writeBlock

type JBloom = BF.Bloom BlockNumber
data JInfo = JInfo 
    { getJournal :: JU.Journal
    , getBloomFilter :: JBloom
    }

data TFile = TFile {
    fHandle :: FH.FHandle ,
    jQueue  :: IORef (DQ.BankersDequeue JInfo) ,
    failedQueue :: IORef (DQ.BankersDequeue (Unique,JBloom))
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
                    (Nothing , _) -> FH.readBlock (fHandle tf) bn -- read from database file

-- | Returns True if there is a block from bli which is probably in the  list of bloom filters
checkF :: [JBloom] -> [BlockNumber] -> Bool
checkF jbl bli = any id [elemB bn jb | jb <- jbl , bn <- bli]


getJInfoList :: JId -> JId -> DQ.BankersDequeue JInfo -> [JInfo]
getJInfoList id1 id2 q = let li = takeFront (Data.Dequeue.length q) q
                             lli = dropWhile (f id1) li 
                        in
                         takeWhile (f id2) lli
                        where
                         f id a = journalID (getJournal a) /= id

-- TODO:Check  Failure should also check the failure queue for priority and failure . 
-- TODO:Union of BloomFilter
checkFailure :: FileVersion -> FileVersion -> TFile -> [BlockNumber] -> IO Bool 
checkFailure oldfv newfv tf bli = do
    q <- readIORef (jQueue tf)
    let jli = getJInfoList oldfv newfv q
    let jbl = map (getBloomFilter) jli
    let b1 = checkF jbl bli
    return b1



