import System.HaskDB.Transactions 
import qualified System.HaskDB.FileHandling as FH
import qualified Data.ByteString as BS 
import qualified Data.ByteString.Char8 as BSC 

rand size = BS.take size $ BSC.pack $ take size $ cycle "This is a test data"
rand2 size = BS.take size $ BSC.pack $ take size $ cycle "Hello"

test = do 
    writeBlockT 0 $ rand 4096 
    writeBlockT 1 $ rand 4096 
    writeBlockT 2 $ rand2 4096 
    writeBlockT 3 $ rand 4096 
    writeBlockT 4 $ rand2 4096 

test2 = do 
    readBlockT  0


main = do 
    newDB <- openTF "test.dat"
    runTransaction test newDB 
    {-runTransaction test newDB -}
    result <- runTransaction test2 newDB 
    print result 
    
