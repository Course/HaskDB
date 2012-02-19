{-# LANGUAGE ExistentialQuantification #-}
module Transactions where 
import Control.Concurrent 
import qualified Data.ByteString as BS 
import qualified FileHandling as FH 
import Data.Maybe 

data FileVersion = FileVersion Int  
    deriving (Eq,Show) 
data BlockNumber = BlockNumber Int 
data BlockData = BlockData BS.ByteString 
type FileInformation = FH.FHandle 

readBlock :: FileInformation -> BlockNumber -> IO a 
readBlock = undefined  -- To be changed according to file handling api 


data FWT a = 
    WDone a | 
    forall x . WReadBlock BlockNumber (x -> FWT a) |
    forall x . WWriteBlock BlockNumber x (FWT a)               -- Think of way to carry forward that a write is done and the new version 


-- | FRT monad . It represents read only transactions . We have differentiated this with write operations as we can provide extra flexibility for read only transactions. They never block . 
data FRT a =                                               -- Read Only Transaction . Never Block . Always go on 
    RDone a |
    forall x. RReadBlock BlockNumber (x -> FRT a)

instance Monad FRT where 
    return = RDone
    m >>= f = case m of 
        RDone a -> f a 
        RReadBlock bn c -> RReadBlock bn (\i -> c i >>= f)

rRead :: BlockNumber -> FRT a 
rRead = flip RReadBlock return 

getFileVersion :: FileInformation -> IO FileVersion 
getFileVersion = undefined 


readOnlyT :: FRT a -> FileInformation -> IO (Maybe a)   -- Can be improved if we know early that the fileversion is changed 
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

