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

-- | FRT monad . It represents read only transactions . We have differentiated this with write operations as we can provide extra flexibility for read only transactions. They never block . 
-- THINK 
-- Can we combine both the monads and still provide the flexibility for read only transactions 
data FRT a =  
    RDone a |
    forall x. RReadBlock BlockNumber (x -> FRT a)

-- | Monad instance of FRT. 
-- THINK 
-- Make it a monad transformer 
instance Monad FRT where 
    return = RDone
    m >>= f = case m of 
        RDone a -> f a 
        RReadBlock bn c -> RReadBlock bn (\i -> c i >>= f)

-- | FRT monad only exposes this function to read blocks . The set of bundled read operations using this should be passed to readOnlyT to  actually perform the IO . 
rRead :: BlockNumber -> FRT a 
rRead = flip RReadBlock return 


-- | This function performs the read Only transaction . Read Only transactions never block . They might fail if another write process has changed the file version 
-- THINK  
-- Can be improved if we know early that the fileversion is changed . Also the transaction should not fail incase the blocks modified in the new version and the blocks read are  disjoint .
-- Read on multiple files . Transaction should succeed  only when the read transactions on each file has successfully completed . 
readOnlyT :: FRT a -> FileInformation -> IO (Maybe a)   
readOnlyT ft fh = do 
        fileVersion <- getFileVersion fh 
        output <- trans ft fh 
        newFileVersion <- getFileVersion fh 
        if fileVersion == newFileVersion then 
            return $ Just output 
            else return Nothing 
    where 
        trans (RDone a) _ = return a 
        trans (RReadBlock bn c) fh = do 
            content <- readBlock fh bn 
            trans (c content) fh 

-- | Read Write Transactions . Although pure reads can also be performed using this but should be avoided as FRT is more optimized for operations which only perform read .
-- This will finally change the version of the file. It will also maintain journal logs for it to rollback in case of failed transaction . 
-- THINK 
-- Further separating pure write operations can further optimize the transaction . 
data FWT a = 
    WDone a | 
    forall x . WReadBlock BlockNumber (x -> FWT a) |
    forall x . WWriteBlock BlockNumber x (FWT a)               -- Think of way to carry forward that a write is done and the new version 

-- | Monad instance of FWT 
-- THINK 
-- Make it a monad transformer 
instance Monad FWT where 
    return = WDone 
    m >>= f = case m of 
        WDone a -> f a 
        WReadBlock bn c -> WReadBlock bn (\i -> c i >>= f)
        WWriteBlock bn x c ->  WWriteBlock bn x (c >>= f)

-- | This function will perform the read write transactions . 
-- THINK 
-- How to implement the journal backup of write blocks to rollback in case of failure . 
-- How to change the versions of the file . 
-- Multiple files read write transactions .
readWriteT :: FWT a -> FileInformation -> IO (Maybe a)
readWriteT ft fh = do 
    fileversion <- getFileVersion fh 
    return Nothing 
    
-- | Combining both the monads 
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
                    Just x -> return x 
            -- This means it is the first write operation , so create a journal and all . You might want to change bool to Maybe fh , where fh can be handle to journal file .. 
            -- So if  it is Nothing then it means the  operations are read only , otherwise they are read write 
        writeLog des bn x 
        writeBlock fh bn x 
        trans c fh (Just des)
            

        






