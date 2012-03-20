\begin{code}
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
import Data.Word
import Data.Serialize

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

-- TODO : Heap implementation of transactions is much better as  only operation to support is 
-- getMinFileVersion and insert a transaction and delete a transaction.
data TFile = TFile {
    fHandle :: FH.FHandle
    , jQueue  :: IORef (DQ.BankersDequeue JInfo)
    , failedQueue :: IORef (DQ.BankersDequeue (Unique,FileVersion))
    , transactions :: IORef (DQ.BankersDequeue (Unique,FileVersion))
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
{-checkF :: [JBloom] -> [BlockNumber] -> Bool-}
{-checkF jbl bli = any id [elemB bn jb | jb <- jbl , bn <- bli]-}
checkF = undefined


getJInfoList :: JId -> JId -> DQ.BankersDequeue JInfo -> [JInfo]
getJInfoList id1 id2 q = let li = takeFront (Data.Dequeue.length q) q
                             lli = dropWhile (f id1) li 
                        in
                         takeWhile (f id2) lli
                        where
                         f id a = journalID (getJournal a) /= id

-- | checkFailure returns True when transaction has to fail. 
-- The transaction will fail only in the following cases : - 
-- 1. The set of  readBlocks of the current transaction and writeBlocks of the failed transaction is not disjoint
-- 2. The set of the readBlocks of current transaction and the blocks changed in between old fileversion and new fileversion is not disjoint
-- TODO:Union of BloomFilter
checkFailure :: FileVersion -> FileVersion -> TFile -> [BlockNumber] -> IO Bool 
checkFailure oldfv newfv tf bli = do
    fq <- readIORef (failedQueue tf)
    let fli = map snd (takeFront (Data.Dequeue.length fq) fq)
    case checkF fli bli of
        True -> return True
        False -> do
            q <- readIORef (jQueue tf)
            let jli = getJInfoList oldfv newfv q
            let jbl = map (getBloomFilter) jli
            let b1 = checkF jbl bli
            return b1

-- | Interface to maintain list of read Blocks 
-- Only that much data is kept in the memory which can be fit in a file 
-- Rest is written on to the hard disk . 

-- Size of empty list is 8 bytes 
data BlockList = BlockList {
    blocks :: [Word64]
    , size :: Int 
    , transH :: FH.FHandle
    }

addBlock :: Word64 -> BlockList -> IO BlockList
addBlock b bl = do 
    let s = size bl 
    let blkSize = FH.blockSize $ transH bl 
    let blks = blocks bl
    if s + 8 > blkSize 
        then do  
            FH.appendBlock (transH bl) $ encode blks 
            return $ BlockList [b] 16 (transH bl)
        else 
            return $ BlockList (b:blks) (s+8) (transH bl)

getBlock :: BlockList -> IO (Maybe Word64,BlockList)
getBlock bl = do 
    let s = size bl 
    let blkSize = FH.blockSize $ transH bl 
    let blks = blocks bl 
    if s == 8 
        then do  
            bs <- FH.getLastBlock $ transH bl
            case bs of 
                Just bs -> do 
                    let ls = decode bs
                    case ls of 
                        Right (x:xs) -> return (Just x,BlockList xs ((1 + Prelude.length xs)*8) (transH bl))
                        Left err -> return (Nothing , BlockList [] 8 (transH bl))
                Nothing -> return (Nothing , BlockList [] 8 (transH bl))
        else do  
            let (b:bs) = blks 
            return (Just b ,  BlockList bs (s-8) (transH bl))


        

\end{code}
