{-# LANGUAGE ExistentialQuantification #-}
module Transactions where 
import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified FileHandling as FH 
import Data.Maybe 

---- Some definitions to change the datatype afterwards . 
-- Two types of FileVersion to distinguish whether the changes are comitted or not
-- Or should we check the existence of a journal file to find out the status of the file
data FileVersion = Committed Int |
                   Uncomitted Int 
    deriving (Eq,Show) 
data BlockNumber = BlockNumber Int 
data BlockData = BlockData BS.ByteString 
data LogDescriptor = LogDescriptor 
type FileInformation = FH.FHandle 

readBlock :: FileInformation -> BlockNumber -> IO a 
readBlock = undefined  -- To be changed according to file handling api 
writeBlock :: FileInformation -> BlockNumber -> a -> IO a 
writeBlock = undefined  -- To be changed according to file handling api 

-- | Reads the current file version from the disk . 
getFileVersion :: FileInformation -> IO FileVersion 
getFileVersion = undefined 

-- | Used to change the current file version . This actually writes to a journal file for it to persist even in case of system crash 
changeFileVersion :: FileInformation -> FileVersion -> IO ()
changeFileVersion = undefined 

data FT a = 
    Done a |
    forall x . ReadBlock BlockNumber (x -> FT a) |
    forall x . WriteBlock BlockNumber x (FT a)

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

runTransaction :: FT a -> FileInformation -> IO (Maybe a) 
runTransaction ft fh = do 
    fileVersion <- getFileVersion fh 
    (output ,rw) <- trans ft fh Nothing 
    if not.isNothing $ rw  then 
        -- Final for read write transaction 
        -- Operations like cleaning up journal or rollback in case of failure etc 
        return $ Just output 
        else do   
                -- Only read operation in transaction 
                newFileVersion <- getFileVersion fh  
                if fileVersion == newFileVersion then 
                    return $ Just output 
                    else return Nothing 
  where 
    trans :: FT a -> FileInformation -> Maybe LogDescriptor  -> IO (a,Maybe LogDescriptor)
    trans (Done a) _ d = return (a,d) 
    trans (ReadBlock bn c) fh d = do 
        val <- readBlock fh bn 
        trans (c val) fh d 
    trans (WriteBlock bn x c) fh d = do 
        des <- case d of 
                    Nothing -> newLogDescriptor 
                    Just oldDes -> return oldDes
            -- This means it is the first write operation , so create a journal and all . You might want to change bool to Maybe fh , where fh can be handle to journal file .. 
            -- So if  it is Nothing then it means the  operations are read only , otherwise they are read write 
        writeLog des bn x 
        writeBlock fh bn x 
        trans c fh (Just des)
            

        






