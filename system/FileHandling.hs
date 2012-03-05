module FileHandling where 
import System.IO
import GHC.Word
import qualified Data.ByteString as BS 
import qualified Data.ByteString.Char8 as BSC 
import Control.Concurrent 
import Control.Applicative
-- | New File Handle with blocksize in Bytes stored in the Handle 
data FHandle = FHandle {
    filePath :: FilePath , 
    blockSize :: Int ,
    handle :: Handle
    }

-- | Opens the file given path , mode and BlockSize and returns the file handle 
openF :: FilePath -> IOMode -> Int -> IO FHandle 
openF fp m bs = openBinaryFile fp m >>= return.FHandle fp bs

-- | Closes the file Handle 
closeF :: FHandle -> IO ()
closeF = hClose.handle 

-- | Given the File Handle and block number , reads the block and returns it 
readBlock :: FHandle -> Integer -> IO BS.ByteString 
readBlock fh i = do 
    hSeek (handle fh) AbsoluteSeek $ (toInteger $ blockSize fh)*i 
    BS.filter (/=000) <$> BS.hGet (handle fh) (blockSize fh) -- filters out \NUL character . 

-- | Given the File Handle and block number and data to be written in ByteString , writes the given block. Adds \NUL if data is less than the block size .  
writeBlock :: FHandle -> Integer -> BS.ByteString -> IO () 
writeBlock fh i bs = do 
    currentPos <- hTell (handle fh) 
    hSeek (handle fh) AbsoluteSeek $ (toInteger $ blockSize fh)*i 
    BS.hPut (handle fh) (BS.take (blockSize fh) (BS.append bs (BS.pack (take (blockSize fh) $ cycle [000 :: GHC.Word.Word8] ))))
    hSeek (handle fh) AbsoluteSeek currentPos             -- Necessary because concurrent use of appendBlock and writeBlock was resulting in overwriting of block next to where writeBlock was called with append block . 

-- | Appends a block at the end of the file 
appendBlock :: FHandle -> BS.ByteString -> IO Integer
appendBlock fh bs = do 
    hSeek (handle fh) SeekFromEnd 0 
    currentPos <- hTell (handle fh)
    BS.hPut (handle fh) (BS.take (blockSize fh) (BS.append bs (BS.pack (take (blockSize fh) $ cycle [000 :: GHC.Word.Word8] ))))
    return.floor $ (fromIntegral currentPos) / (fromIntegral $ blockSize fh)


-- | Flushes the buffer to hard disk 
flushBuffer :: FHandle -> IO () 
flushBuffer fh = hFlush $ handle fh 

-- | Zeroes out the file . 
truncateF :: FilePath -> IO ()
truncateF fp = do 
    p <- openF fp WriteMode 1024 
    closeF p 

test = do 
    c <- openF "abc.b" WriteMode 1024   -- Truncates to zero length file 
    closeF c
    p <- openF "abc.b" ReadWriteMode 1024 
    forkIO $ do 
        sequence_ $ map (\s -> appendBlock p (BSC.pack (show s))) [1..100]
    forkIO $ do 
        sequence_ $ map (\s -> appendBlock p (BSC.pack (show s))) [101..200]
    appendBlock p (BSC.pack "Hello How are you" )
    writeBlock p 0 (BSC.pack "First Block")
    bs <- appendBlock p (BSC.pack "check")
    print bs
    threadDelay 1000 -- To keep thread blocked and not close the handle before data is being written . 
    flushBuffer p 
    closeF p
    p <- openF "abc.b" ReadMode 1024
    x <- sequence $ map (readBlock p) [0..500]
    print x
    closeF p

