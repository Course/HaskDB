> module System.HaskDB.Journal where
> 
> import System.IO
> import System.Directory
> import System.Random
> import Control.Applicative
> import Data.Map as Map
> import Data.Serialize
> import Data.Text
> import Data.Maybe
> import Data.ByteString as BS
> import qualified Data.ByteString.Char8 as BSC 
> import qualified System.HaskDB.FileHandling as FH 
> import System.HaskDB.FileHeader
> import Data.IORef

Whenever a block is to be written to the main Transaction File , we can essentially follow one of the following two approaches: - 

* Copy the original block to the Journal and try writing the changed block to the main file. If the transaction goes through successfully , we can discard the Journal . This approach is simpler to implement and we started with this approach in the beginning. But we came to realize that this would be highly inefficient .
* Our current approach is based on an idea similar to Write Ahead Logging in Sqlite. Instead of writing the "old data" to journal , we write the new data first to the Journal and then eventually move it to the main file. But this approach means the main file alone is no more the only place to read data from. The main file with all the journals together really give us the latest picture of our data.

A Journal is a temporary file defined per transaction.It is used to keep track of the blocks changed by a transaction. It stores the new data that needs to be eventually written to the main database. 

Following are some trivial types for journal 

> type JHandle = FH.FHandle
> type OldBlock = Integer
> type JId = Integer

A journal object requires a unique identifier , file handles of the header 
file ,  journal file and the database file and an index storing a mapping of block number in the
database to block number in journal file.

> data Journal = Journal { journalID :: JId -- filename prefix of Journal
>                        , hHandle :: FH.FHandle -- Handle for the header file
>                        , jHandle :: FH.FHandle -- Handle for the Journal file
>                        , dHandle :: FH.FHandle -- Handle for the database 
>                        , oldBlocks :: Map Integer Integer 
>                        -- Map from block number in database to block number
>                        -- of the journal
>                        }

Closing the Journal object requires closing the handle of both the header file
as well as the journal file.

> closeJournal j = do 
>     FH.closeF $ jHandle j 
>     FH.closeF $ hHandle j

> -- | Find all the journals present on the disk 
> findJournals :: FilePath -> IO [JId]
> findJournals dp = do
>     files <- getDirectoryContents dp
>     return [ read (Data.Text.unpack id)
>            | file <- files
>            , id <- maybeToList (stripSuffix (Data.Text.pack ".header") 
>               (Data.Text.pack file))
>            ]
>     -- return a list of JIds
> 
> -- | Create a new unique Journal file
> newJournal :: FH.FHandle -> IO Journal
> newJournal dh = do
>                   gen <- newStdGen
>                   jid <- atomicModifyIORef (FH.journalId dh) (\a -> (a+1,a))
>                   let filename = show jid
>                   let header = filename ++ ".header"
>                   let journal = filename ++ ".journal"
>                   headerHandle <- FH.openF header ReadWriteMode 1024
>                   journalHandle <- FH.openF journal ReadWriteMode 1024
>                   let fJournal = Journal { journalID = jid
>                                          , hHandle = headerHandle
>                                          , jHandle = journalHandle
>                                          , dHandle = dh
>                                          , oldBlocks = Map.empty
>                                          }
>                   return fJournal
> 
> -- | Build a Journal from the journal file on disk
> buildJournal :: JId -> FH.FHandle -> IO Journal
> buildJournal id dh = do
>                       let filename = show id
>                       let header = filename ++ ".header"
>                       let journal = filename ++ ".journal"
>                       headerHandle <- FH.openF header ReadWriteMode 1024
>                       journalHandle <- FH.openF journal ReadWriteMode 1024
>                       retreivedMap <- readHeader headerHandle
>                       let fJournal = Journal { journalID = id
>                                          , hHandle = headerHandle
>                                          , jHandle = journalHandle
>                                          , dHandle = dh
>                                          , oldBlocks = retreivedMap
>                                          }
>                       return fJournal
> 
> 
> -- | Reads all the blocks in a Journal
> readAllBlocksFrom :: Journal -> IO [BS.ByteString]
> readAllBlocksFrom j = undefined
> 
> -- | Given a block number in the database file , read it from the Journal
> readFromJournal :: Journal -> Integer -> IO (Maybe BS.ByteString)
> readFromJournal j bn = do
>                         let l = Map.lookup bn (oldBlocks j)
>                         case l of
>                             Just val -> Just <$> FH.readBlock  (jHandle j)  val
>                             Nothing -> return Nothing
> 
> -- | Writes the header to the header file
> writeHeader :: Journal -> IO ()
> writeHeader j = do
>   let s = encode (oldBlocks j)
>   FH.writeAll (hHandle j) s
> 
> -- | Reads the header information from the header file
> readHeader :: FH.FHandle -> IO (Map Integer Integer)
> readHeader fh = do
>   val <- FH.readAll fh
>   let (Right m) = decode (val)  --Either String a
>   return m
> 
> -- | Write to a journal given block number and blockData
> writeToJournal :: Journal -> Integer -> BS.ByteString -> IO Journal
> writeToJournal j bn bd = do
>  val <- FH.appendBlock  (jHandle j) bd
>  let newMap = insert bn val (oldBlocks j)
>  let fJournal = Journal { journalID = journalID j
>                      , hHandle = hHandle j
>                      , jHandle = jHandle j
>                      , dHandle = dHandle j
>                      , oldBlocks = newMap
>                      }
>  writeHeader fJournal
>  return fJournal 

> -- | To zero out the journal file
> resetJournal :: Journal -> IO ()
> resetJournal = FH.truncateF . jHandle
> 
> -- | Commit the Journal by writing the new FileVersion to its header
> -- change this later
> commitJournal :: Journal -> IO ()
> commitJournal j = changeFileVersion (dHandle j)
> 
> 
> -- | Replay the data from the Journal to bring back the database into a consistent
> -- state in case of a power failure
> -- Read every block from the journal and write to the database 
> replayJournal :: Journal -> IO ()
> replayJournal j = do
>  let li = toAscList $ oldBlocks j --potential speedup if ascending?
>  sequence_ $ Prelude.map readAndWrite li
>  where
>  readAndWrite :: (Integer,Integer) -> IO()
>  readAndWrite (bn,jbn) = do
>          maybebd <- readFromJournal j bn
>          case maybebd of 
>              Just bd -> FH.writeBlock (dHandle j) bn bd
>              Nothing -> return ()

> test = do 
>     d <- FH.openF "abc.b" ReadWriteMode 1024   -- Truncates to zero length file 
>     j <- newJournal d
>     k <- writeToJournal j 256 (BSC.pack "Block number 256")
>     l <- writeToJournal k 666 (BSC.pack "Block number 666")
>     let x = Map.lookup 256 (oldBlocks l)
>     let y = Map.lookup 666 (oldBlocks l)
>     r1 <- readFromJournal l 256
>     r2 <- readFromJournal l 666
>     case r1 of 
>         Just v -> BS.putStrLn v
>         _ -> return ()


