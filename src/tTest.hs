import System.HaskDB.Transactions 
import System.HaskDB.TransactionFH
import System.HaskDB.FileHeader
import System.HaskDB.FileHandling
import qualified System.HaskDB.FileHandling as FH
import qualified Data.ByteString as BS 
import qualified Data.ByteString.Char8 as BSC 
import Control.Concurrent 
import Control.Applicative

rand size = BS.take size $ BSC.pack $ take size $ cycle "This is a fttest data"
rand2 size = BS.take size $ BSC.pack $ take size $ cycle "Hello"

fttest = do 
    writeBlockT 0 $ rand 4096 
    writeBlockT 1 $ rand 4096 
    writeBlockT 2 $ rand2 4096 
    writeBlockT 3 $ rand 4096 
    writeBlockT 4 $ rand2 4096 

fttest2 = do 
    readBlockT  0

fttest3 = do
    writeBlockT 0 $ rand2 4096

main = do 
    newDB <- openTF "fttest.dat"
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    forkIO $ runTransaction fttest newDB >> return ()
    t <- getFileVersion (fHandle  newDB)
    print t
    {-sequencer newDB-}
    
