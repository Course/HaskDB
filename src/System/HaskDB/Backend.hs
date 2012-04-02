{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
module System.HaskDB.Backend where 
import qualified Data.Map as M
import Control.Monad.State
import Control.Applicative
import Data.Maybe


class (Show a,Monad m) => Backend b m a | b -> m where 
    data Handle :: * -> * 
    type BlockNumber :: *       -- I want BlockNumber to have constraint Eq 
    open :: FilePath -> Mode -> m (Handle b)
    close :: Handle b -> m () 
    readBlock :: (Eq BlockNumber) => Handle b -> BlockNumber -> m a 
    writeBlock :: (Eq BlockNumber) => Handle b -> BlockNumber -> a -> m ()
    sync :: Handle b -> m ()


-- Pure Functions to simulate file system . This implementation only support one handle per file .
-- Pessimistic implementation so data is not written to the simulated disk until sync is not called .
data TestDisk a = TestDisk {
    disk :: M.Map FilePath (File a)
    , buffers :: M.Map FilePath (File a) 
    , bufferSize :: Int -- Current buffer Size 
    , openFiles :: M.Map FilePath Mode
    }

data File a = File {
    blocks :: M.Map Int a 
    , size :: Int 
    }

data Mode = ReadWrite | Read | Write 

instance Show a => Backend (TestDisk a)  (State (TestDisk a)) a where 
    data Handle (TestDisk a) = Handle FilePath 
    type BlockNumber = Int 
    open fp m = do 
        disk <- get
        put $ openFile disk fp m 
        return $ Handle fp
    close (Handle fp) = do 
        disk <- get 
        put $ closeFile disk fp
    readBlock (Handle fp) bn = do 
        t <- get 
        case readB t fp bn buffers of 
            Nothing -> error "Block Not Found"
            Just a -> return a
    writeBlock (Handle fp) bn a = do 
        t <- get 
        put $ writeB t fp bn a 
    sync (Handle fp) = do  
        t <- get 
        put $ flushBuffer t fp 
 

-- Opens the file . If file is not present then creates it . Also create the buffer space.
openFile :: TestDisk a -> FilePath -> Mode -> TestDisk a
openFile t fp md = let 
                      d = if M.member fp (disk t) 
                             then t
                             else t { disk = M.insert fp (File M.empty 0) (disk t)
                                    , buffers = M.insert fp (File M.empty 0) (buffers t)}
                      opfs = openFiles d 
                      newOpfs = if M.member fp opfs then opfs else M.insert fp md opfs 
                   in d {openFiles = newOpfs}

-- Flushes the buffer and closes the handle .
closeFile ::  TestDisk a -> FilePath -> TestDisk a
closeFile t fp = let opfs = openFiles t 
                     newOpfs = M.delete fp opfs 
                     d = flushBuffer t fp 
                     in d {openFiles = newOpfs , buffers = M.delete fp (buffers t)}

-- Writes the buffers of the given handle to the disk 
flushBuffer :: TestDisk a -> FilePath -> TestDisk a
flushBuffer t fp = let file = M.findWithDefault undefined fp (disk t) -- Sure that undefined will never be called
                       fsize = size file 
                       buffer = M.findWithDefault undefined fp (buffers t)
                       bsize = size buffer
                       blks = M.union (blocks buffer) (blocks file)
                    in t { disk = M.insert fp (File blks (fsize + bsize)) (disk t) 
                         , buffers = M.insert fp (File M.empty 0) (buffers t)}


isClosed :: TestDisk a -> FilePath -> Bool 
isClosed t fp = M.member fp (openFiles t )

-- Reads from the buffers and if not present then reads from the disk 
readB :: TestDisk a -> FilePath -> Int -> (TestDisk a -> M.Map FilePath (File a)) -> Maybe a  
readB t fp bn from = case join (M.lookup bn . blocks <$> M.lookup fp (from t)) of  
    Nothing -> readB t fp bn disk 
    Just a -> Just a 
    

-- Writes to the buffers  
writeB :: TestDisk a -> FilePath -> Int -> a -> TestDisk a
writeB t fp bn v = let file = (\(File b s)  -> File (M.insert bn v b) s)  <$> M.lookup fp (buffers t)
                   in maybe t (\a -> t {buffers = M.insert fp a (buffers t) , bufferSize = bufferSize t + (sizeOf a)}) file

sizeOf _ = 1


   



