module System.HaskDB.TransactionFH where 
import qualified System.HaskDB.FileHandling as FH 
import qualified Data.Dequeue as DQ 
import Data.IORef 
import qualified Data.BloomFilter as BF 
import System.HaskDB.Journal 
import qualified Data.ByteString as BS 

data BlockData = BlockData BS.ByteString 
type BlockNumber = Integer
data LogDescriptor = LogDescriptor 
type FileInformation = FH.FHandle 
type FileVersion = BS.ByteString

readBlock = FH.readBlock
writeBlock = FH.writeBlock

type JBloom = BF.Bloom BlockNumber
data JInfo = JInfo 
    { getJournal :: Journal
    , getBloomFilter :: JBloom
    }

data TFile = TFile {
    fHandle :: FH.FHandle ,
    jQueue  :: IORef (DQ.BankersDequeue JInfo) ,
    failedQueue :: IORef (DQ.BankersDequeue JInfo)
    }
