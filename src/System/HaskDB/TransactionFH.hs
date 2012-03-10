module System.HaskDB.TransactionFH where 
import qualified System.HaskDB.FileHandling as FH 
import qualified Data.Dequeue as DQ 
import Data.IORef 
import qualified Data.BloomFilter as BF 

type BlockNumber = Integer
type JBloom = BF.Bloom BlockNumber
data JInfo = JInfo 
    { getJournal : Journal
    , getBloomFilter : JBloom
    }

data TFile = TFile {
    handle :: FH.FHandle ,
    jQueue  :: IORef (DQ.BankersDequeue JInfo)
    failedQueue :: IORef (DQ.BankersDequeue JInfo)
    }
