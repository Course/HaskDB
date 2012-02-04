module FileHandling where 
import System.IO
import Data.ByteString as BS 
import Data.ByteString.Char8 as BSC 
-- | New File Handle with blocksize in Bytes stored in the Handle 
data FHandle = FHandle {
    blockSize :: Int ,
    handle :: Handle
    }

-- | Opens the file given path , mode and BlockSize and returns the file handle 
openF :: FilePath -> IOMode -> Int -> IO FHandle 
openF fp m bs = openBinaryFile fp m >>= return.FHandle bs

-- | Closes the file Handle 
closeF :: FHandle -> IO ()
closeF = hClose.handle 

-- | Given the File Handle and block number , reads the block and returns it 
readBlock :: FHandle -> Integer -> IO BS.ByteString 
readBlock fh i = do 
    hSeek (handle fh) AbsoluteSeek $ (toInteger $ blockSize fh)*i 
    BS.hGet (handle fh) (blockSize fh) 

-- | Given the File Handle and block number and data to be written in ByteString , writes the given block 
writeBlock :: FHandle -> Integer -> BS.ByteString -> IO () 
writeBlock fh i bs = do 
    hSeek (handle fh) AbsoluteSeek $ (toInteger $ blockSize fh)*i 
    BS.hPut (handle fh) (BS.take (blockSize fh) bs) 

main = do 
    p <- openF "abc.b" WriteMode 1024 
    writeBlock p 0 (BSC.pack "hell no")
    closeF p

