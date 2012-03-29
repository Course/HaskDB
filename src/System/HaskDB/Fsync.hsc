{-# LANGUAGE ForeignFunctionInterface #-}
module System.HaskDB.Fsync (sync) where

import Foreign.C.Error (throwErrnoIfMinus1_)
import Foreign.C.Types (CInt)
import System.Posix.Types (Fd(..))
import System.Posix.IO (handleToFd)
import System.IO 

#include <unistd.h>

foreign import ccall "fsync"
       c_fsync :: CInt -> IO CInt

fsync :: Fd -> IO ()
fsync (Fd fd) =  throwErrnoIfMinus1_ "fsync" $ c_fsync fd

sync :: Handle -> IO ()
sync fh = do 
    fd <- handleToFd fh  
    fsync fd


