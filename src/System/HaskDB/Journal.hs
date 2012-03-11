module System.HaskDB.Journal where

import System.IO
import System.Directory
import System.Random
import Control.Applicative
import Data.Map as Map
import Data.Serialize
import Data.Text
import Data.Maybe
import Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC 
import qualified System.HaskDB.FileHandling as FH 
import System.HaskDB.FileHeader

-- | A Journal is temporary file to keep track of data soon to be modified in the database.
-- Only one Journal per transaction?
-- Journal is a collection of "old blocks" in a particular transaction

-- | Do we need to distinguish between database handle and journal handle?
type JHandle = FH.FHandle
type OldBlock = Integer
type JId = Integer

data Journal = Journal { journalID :: JId      -- filename prefix of the journal
                       , hHandle :: FH.FHandle -- Handle for the header file
                       , jHandle :: FH.FHandle -- Handle for the Journal file
                       , dHandle :: FH.FHandle -- Handle for the database File
                       , oldBlocks :: Map Integer Integer -- Map from block number in database to block number of the journal
                       }

-- | An oldBlock  is the "old data" that is to be replaced by the writeblock succeeding it
{-data OldBlock = OldBlock { id :: Int-}
                         {-, blockNumber :: Int-}
                         {-, blockData :: BS-}
                         {-}-}

-- | Find existing Journals present on the disk 
findJournals :: FilePath -> IO [JId]
findJournals dp = do
    files <- getDirectoryContents dp
    return [ read (Data.Text.unpack id)
           | file <- files
           , id <- maybeToList (stripSuffix (Data.Text.pack ".header") (Data.Text.pack file))
           ]
    -- return a list of JIds


-- | Create a new unique Journal file
newJournal :: FH.FHandle -> IO Journal
newJournal dh = do
                  gen <- newStdGen
                  latestfv <- getFileVersion dh 
                  let jid = latestfv + 1
                  let filename = show jid
                  let header = filename ++ ".header"
                  let journal = filename ++ ".journal"
                  headerHandle <- FH.openF header ReadWriteMode 1024
                  journalHandle <- FH.openF journal ReadWriteMode 1024
                  let fJournal = Journal { journalID = jid
                                         , hHandle = headerHandle
                                         , jHandle = journalHandle
                                         , dHandle = dh
                                         , oldBlocks = Map.empty
                                         }
                  return fJournal

-- | Build a Journal from the journal file on disk
buildJournal :: JId -> FH.FHandle -> IO Journal
buildJournal id dh = do
                      let filename = show id
                      let header = filename ++ ".header"
                      let journal = filename ++ ".journal"
                      headerHandle <- FH.openF header ReadWriteMode 1024
                      journalHandle <- FH.openF journal ReadWriteMode 1024
                      retreivedMap <- readHeader headerHandle
                      let fJournal = Journal { journalID = id
                                         , hHandle = headerHandle
                                         , jHandle = journalHandle
                                         , dHandle = dh
                                         , oldBlocks = retreivedMap
                                         }
                      return fJournal

-- | It reads all the blocks in a journal
readAllBlocksFrom :: Journal -> IO [BS.ByteString]
readAllBlocksFrom j = undefined


-- | Given a block number in the database file , read it from the Journal
readFromJournal :: Journal -> Integer -> IO (Maybe BS.ByteString)
readFromJournal j bn = do
                        let l = Map.lookup bn (oldBlocks j)
                        case l of
                            Just val -> Just <$> FH.readBlock  (jHandle j)  val
                            Nothing -> return Nothing

-- | Writes the header to the header file
writeHeader :: Journal -> IO ()
writeHeader j = do
                  let s = encode (oldBlocks j)
                  FH.writeAll (hHandle j) s

-- | Reads the header information from the header file
readHeader :: FH.FHandle -> IO (Map Integer Integer)
readHeader fh = do
                  val <- FH.readAll fh
                  let (Right m) = decode (val)  --Either String a
                  return m


-- | Write to a journal given block number and blockData
writeToJournal :: Journal -> Integer -> BS.ByteString -> IO Journal
writeToJournal j bn bd = do
                            let d = Map.lookup bn (oldBlocks j) 
                            case d of
                                Just _ -> return j
                                Nothing -> do 
                                             val <- FH.appendBlock  (jHandle j) bd
                                             let newMap = insert bn val (oldBlocks j)
                                             let fJournal = Journal { journalID = journalID j
                                                                    , hHandle = hHandle j
                                                                    , jHandle = jHandle j
                                                                    , dHandle = dHandle j
                                                                    , oldBlocks = newMap
                                                                    }
                                             writeHeader fJournal
                                             return fJournal

-- | To zero out the journal file
resetJournal :: Journal -> IO ()
resetJournal = FH.truncateF . FH.filePath . jHandle

-- | Commit the Journal by writing the new FileVersion to its header
-- TODO: Do it atomically
commitJournal :: Journal -> Integer -> IO ()
commitJournal j newfv = do
    changeFileVersion (hHandle j) newfv
    changeFileVersion (dHandle j) newfv


-- | Replay the data from the Journal to bring back the database into a consistent
-- state in case of a power failure
-- Read every block from the journal and write to the database 

replayJournal :: Journal -> IO ()
replayJournal j = do
                    let li = toAscList $ oldBlocks j --potential speedup if ascending?
                    sequence_ $ Prelude.map readAndWrite li
                    where
                      readAndWrite :: (Integer,Integer) -> IO()
                      readAndWrite (bn,jbn) = do
                                          maybebd <- readFromJournal j bn
                                          case maybebd of 
                                            Just bd -> FH.writeBlock (dHandle j) bn bd
                                            Nothing -> return ()

test = do 
    d <- FH.openF "abc.b" ReadWriteMode 1024   -- Truncates to zero length file 
    j <- newJournal d
    k <- writeToJournal j 256 (BSC.pack "Block number 256")
    l <- writeToJournal k 666 (BSC.pack "Block number 666")
    let x = Map.lookup 256 (oldBlocks l)
    let y = Map.lookup 666 (oldBlocks l)
    r1 <- readFromJournal l 256
    r2 <- readFromJournal l 666
    case r1 of 
        Just v -> BS.putStrLn v
        _ -> return ()

