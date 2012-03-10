{-# LANGUAGE ExistentialQuantification #-}
module System.HaskDB.Transactions where 

import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified System.HaskDB.FileHandling as FH 
import Data.Maybe 
import System.HaskDB.Journal 
import System.HaskDB.TransactionFH 

-- | Some definitions to change the datatype afterwards . 

data BlockData = BlockData BS.ByteString 
data LogDescriptor = LogDescriptor 
type FileInformation = FH.FHandle 
type FileVersion = BS.ByteString

readBlock = FH.readBlock
writeBlock = FH.writeBlock
readBlockJ :: TFile -> BlockNumber -> IO a 
readBlockJ = undefined  -- To be changed according to file handling api 
writeBlockJ :: TFile -> BlockNumber -> a -> IO a 
writeBlockJ = undefined  -- To be changed according to file handling api 


-- | Reads the current file version from the disk . 
getFileVersion :: FileInformation -> IO FileVersion 
getFileVersion = undefined 

-- | Used to change the current file version . This actually writes to a journal file for it to persist even in case of system crash 
changeFileVersion :: FileInformation -> FileVersion -> IO ()
changeFileVersion = undefined 

data FT a = 
    Done a |
    ReadBlock BlockNumber (BS.ByteString -> FT a) |
    WriteBlock BlockNumber BS.ByteString (FT a)

--  THINK if it is correct and how to define automatically in this case 
instance Monad FT where 
    return = Done 
    m >>= f = case m of 
        Done a -> f a 
        ReadBlock bn c -> ReadBlock bn (\i -> c i  >>= f) 
        WriteBlock bn x c -> WriteBlock bn x (c >>= f)

newLogDescriptor :: IO LogDescriptor 
newLogDescriptor = undefined 

writeLog :: LogDescriptor -> BlockNumber -> a -> IO () 
writeLog = undefined 

runTransaction :: FT a -> TFile -> IO (Maybe a) 
runTransaction ft fh = do 
    fileVersion <- getFileVersion $ handle fh 
    (output ,rw) <- trans ft fh Nothing 
    if not.isNothing $ rw  then 
        -- Final for read write transaction 
        -- Operations like cleaning up journal or rollback in case of failure etc 
        return $ Just output 
        else do   
                -- Only read operation in transaction 
                newFileVersion <- getFileVersion $ handle fh  
                if fileVersion == newFileVersion then 
                    return $ Just output 
                    else return Nothing 
  where 
    trans :: FT a -> TFile -> Maybe Journal -> IO (a,Maybe Journal)
    trans (Done a) _ d = return (a,d) 
    trans (ReadBlock bn c) fh d = do 
        val <- readBlock (handle fh) bn 
        trans (c val) fh d 
    trans (WriteBlock bn x c) fh d = do 
        des <- case d of 
                    Nothing -> newJournal $ handle fh 
                    Just oldDes -> return oldDes
        
            -- This means it is the first write operation , so create a journal and all . You might want to change bool to Maybe fh , where fh can be handle to journal file .. 
            -- So if  it is Nothing then it means the  operations are read only , otherwise they are read write 
        oldData <- readBlock (handle fh) bn 
        writeToJournal des bn oldData
        writeBlock (handle fh) bn x 
        trans c fh (Just des)







