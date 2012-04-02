module System.HaskDB.Fsync (sync) where 
import Foreign.C.Error
import Foreign.C.Types
import qualified System.IO as IO
import qualified System.IO.Error as IO
import GHC.IO.FD (FD(..))
import GHC.IO.Handle.Types (Handle__(..))
import GHC.IO.Handle.Internals (wantWritableHandle)
import Data.Typeable
foreign import ccall "unistd.h fsync" c_fsync :: CInt -> IO CInt
sync :: IO.Handle -> IO ()
sync h = do
  IO.hFlush h
  wantWritableHandle "hSync" h $ fsyncH
    where
      fsyncH Handle__ {haDevice = dev} = maybe (return ()) fsyncD $ cast dev
      fsyncD FD {fdFD = fd} = throwErrnoPathIfMinus1_ "fsync" (show h)
                              (c_fsync fd)
