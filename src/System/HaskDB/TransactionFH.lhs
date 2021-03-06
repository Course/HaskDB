> module System.HaskDB.TransactionFH where 
> 
> import qualified Data.ByteString as BS
> import qualified System.HaskDB.FileHandling as FH 
> import qualified System.HaskDB.FileHeader as FHD
> import qualified Data.Dequeue as DQ 
> import Data.Maybe
> import Data.Dequeue
> import Data.BloomFilter.Easy
> import Data.IORef 
> import qualified Data.BloomFilter as BF 
> import System.HaskDB.Journal as JU
> import qualified Data.ByteString as BS 
> import Data.Unique 
> import Data.Word
> import Data.Serialize
> import Control.Concurrent 
> import Control.Applicative
> import System.IO 
> 
> data BlockData = BlockData BS.ByteString 
> type BlockNumber = Integer
> data LogDescriptor = LogDescriptor 
> type FileInformation = FH.FHandle 
> type FileVersion = Integer
> 
> readBlock = FH.readBlock
> writeBlock = FH.writeBlock
> 
> data Transaction = Transaction {
>     rBlocks :: BlockList 
>     , tType :: TransactionType 
>     }
> 
> data TransactionType = ReadOnly | ReadWrite {
>         bloom :: BF.Bloom BlockNumber
>         , journal :: Journal 
>         }
> 
> 
> type JBloom = BF.Bloom BlockNumber
> data JInfo = JInfo 
>     { getJournal :: JU.Journal
>     , getBloomFilter :: JBloom
>     , getfv :: Integer
>     }
> 
> -- TODO : Heap implementation of transactions is much better as  only operation to support is 
> -- getMinFileVersion and insert a transaction and delete a transaction.
> data TFile = TFile {
>     fHandle :: FH.FHandle -- handle of the database file 
>     , commitSynch :: MVar ()
>     , jQueue  :: IORef (DQ.BankersDequeue JInfo)
>     , failedQueue :: IORef (DQ.BankersDequeue (Integer,FileVersion))
>     , commitedQueue :: IORef (DQ.BankersDequeue JInfo)
>     , transactions :: IORef (DQ.BankersDequeue (Integer,FileVersion))
>     }
> 
> openTF fpath = do 
>     handle <- FH.openF fpath ReadWriteMode 1024 
>     cmVar  <- newMVar () 
>     jQ <- newIORef DQ.empty 
>     fQ <- newIORef DQ.empty 
>     tQ <- newIORef DQ.empty 
>     return $ TFile handle cmVar jQ fQ tQ 
> 
> closeTF tFile = do 
>     FH.closeF $ fHandle tFile 
>     q <- readIORef $ jQueue tFile 
>     closeAll q 
>   where 
>     closeAll q = do 
>         let (a,b) =  popFront q 
>         case a of 
>             Nothing -> return () 
>             Just a -> do JU.closeJournal $ getJournal a
>                          closeAll b    
> 
> 
> 
> 
> 
> checkInBloomFilter bf bn = elemB bn bf
> 
> readBlockFromOwnJournal :: TFile -> BlockNumber -> Transaction -> IO (Maybe BS.ByteString)
> readBlockFromOwnJournal tf bn d = do
>     case (tType d) of 
>       ReadOnly -> return Nothing
>       ReadWrite _ j -> JU.readFromJournal j bn
> 
> -- | First , if it has its own journal , then it should read from that
> -- else read a block from the most recent journal that contains it 
> -- else read it from the database
> 
> 
> readBlockJ :: TFile -> BlockNumber -> Transaction -> IO BS.ByteString
> readBlockJ tf bn d = do
>   r <- readBlockFromOwnJournal tf bn d
>   case r of 
>     Just x -> do
>                return x 
>     Nothing -> do
>         q <- readIORef $ jQueue tf
>         if DQ.null q then do return BS.empty else func q bn
>     where 
>         func q bn = case popFront q of
>                     (Just jInfo, q') ->  
>                         case checkInBloomFilter (getBloomFilter jInfo) bn of
>                             False -> func q' bn
>                             True -> do
>                                 d <- JU.readFromJournal (getJournal jInfo) bn
>                                 case d of
>                                     Just x -> return x
>                                     Nothing -> func q' bn
>                     (Nothing , _) -> do 
>                         FH.readBlock (fHandle tf) bn -- read from database file
> 
> -- | Returns True if there is a block from bli which is probably in the  list of bloom filters
> checkF :: [JBloom] -> BlockList -> IO Bool
> {-checkF jbl bli = any id [elemB bn jb | jb <- jbl , bn <- bli]-}
> checkF [] bli =  return False 
> checkF js bli = do 
>     --print $ checkOneBlock js 0
>     (f,rest) <- getBlock bli 
>     print f
>     --print $ "Bloom List " ++ (show js)
>     --print $ "Block List " ++ (show (blocks bli))
>     case f of 
>         Just x -> if checkOneBlock js (toInteger x) then return True else checkF js rest 
>         Nothing -> return False 
>   where 
>     checkOneBlock [] bl = False 
>     checkOneBlock (f:fs) bl = if elemB bl f then True else checkOneBlock fs bl
> 
> 
> getJInfoList :: JId -> JId -> DQ.BankersDequeue JInfo -> [JInfo]
> getJInfoList id1 id2 q = takeWhile (\q -> (getfv q) /= id1) $ takeBack (Data.Dequeue.length q) q
> 
> -- | checkFailure returns True when transaction has to fail. 
> -- The transaction will fail only in the following cases : - 
> -- 1. The set of  readBlocks of the current transaction and writeBlocks of the failed transaction is not disjoint
> -- 2. The set of the readBlocks of current transaction and the blocks changed in between old fileversion and new fileversion is not disjoint
> -- TODO:Union of BloomFilter
> checkSuccess :: FileVersion -> FileVersion -> TFile -> BlockList -> IO Bool 
> checkSuccess oldfv newfv tf bli = not <$> checkFailure oldfv newfv tf bli 
> checkFailure :: FileVersion 
>              -> FileVersion 
>              -> TFile 
>              -> BlockList
>              -> IO Bool 
> checkFailure oldfv newfv tf bli = do
>     {-fq <- readIORef (failedQueue tf)-}
>     {-let fli = map snd (takeFront (Data.Dequeue.length fq) fq)-}
>     {-case checkF fli bli of-}
>         {-True -> return True-}
>         {-False -> do-}
>             q <- readIORef (jQueue tf)
>             let jli = getJInfoList oldfv newfv q
>             let jbl = map (getBloomFilter) jli
>             print $ map getfv jli 
>             f <- checkF jbl bli
>             print f
>             return f
> 
> -- | Interface to maintain list of read Blocks 
> -- Only that much data is kept in the memory which can be fit in a file 
> -- Rest is written on to the hard disk . 
> 
> -- Size of empty list is 8 bytes 
> data BlockList = BlockList {
>     blocks :: [Word64]
>     , size :: Int 
>     , transH :: FH.FHandle
>     }
> 
> addBlock :: Word64 -> BlockList -> IO BlockList
> addBlock b bl = do 
>     let s = size bl 
>     let blkSize = FH.blockSize $ transH bl 
>     let blks = blocks bl
>     if s + 8 > blkSize 
>         then do  
>             FH.appendBlock (transH bl) $ encode blks 
>             return $ BlockList [b] 16 (transH bl)
>         else 
>             return $ BlockList (b:blks) (s+8) (transH bl)
> 
> getBlock :: BlockList -> IO (Maybe Word64,BlockList)
> getBlock bl = do 
>     let s = size bl 
>     let blkSize = FH.blockSize $ transH bl 
>     let blks = blocks bl 
>     if s == 8 
>         then do  
>             bs <- FH.getLastBlock $ transH bl
>             case bs of 
>                 Just bs -> do 
>                     let ls = decode bs
>                     case ls of 
>                         Right (x:xs) -> return (Just x,BlockList xs ((1 + Prelude.length xs)*8) (transH bl))
>                         Left err -> return (Nothing , BlockList [] 8 (transH bl))
>                 Nothing -> return (Nothing , BlockList [] 8 (transH bl))
>         else do  
>             let (b:bs) = blks 
>             return (Just b ,  BlockList bs (s-8) (transH bl))


