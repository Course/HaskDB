-- | A Journal is temporary file to keep track of data soon to be modified in the database.
-- Only one Journal per transaction?
-- Journal is a collection of "old blocks" in a particular transaction
module Journal where
import System.IO
import System.Random
import Data.Map as Map
import Data.Serialize
import Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC 
import qualified FileHandling as FH 

-- | Do we need to distinguish between database handle and journal handle?
type JHandle = FH.FHandle
type OldBlock = Integer
type JId = String

data Journal = Journal { journalID :: JId
                       , hHandle :: FH.FHandle -- Handle for the header file
                       , jHandle :: FH.FHandle -- Handle for the Journal file
                       , dHandle :: FH.FHandle -- Handle for the database File
                       , oldBlocks :: Map Integer Integer      -- List of oldBlocks
                       }

-- | An oldBlock  is the "old data" that is to be replaced by the writeblock succeeding it
{-data OldBlock = OldBlock { id :: Int-}
                         {-, blockNumber :: Int-}
                         {-, blockData :: BS-}
                         {-}-}

-- | Find existing Journals present on the disk 
findJournals :: IO [FilePath]
findJournals = undefined

-- | Create a new unique Journal file
newJournal :: FH.FHandle -> IO Journal
newJournal dh = do
                  gen <- newStdGen
                  let filename = Prelude.take 20 (randomRs ('a','z') gen)
                  let header = filename ++ ".header"
                  let journal = filename ++ ".journal"
                  headerHandle <- FH.openF header ReadWriteMode 1024
                  journalHandle <- FH.openF journal ReadWriteMode 1024
                  let fJournal = Journal { journalID = filename
                                         , hHandle = headerHandle
                                         , jHandle = journalHandle
                                         , dHandle = dh
                                         , oldBlocks = Map.empty
                                         }
                  return fJournal

-- | Build a Journal from the journal file on disk
readJournal :: JId -> FH.FHandle -> IO Journal
readJournal id dh = do
                      let filename = id
                      let header = filename ++ ".header"
                      let journal = filename ++ ".journal"
                      headerHandle <- FH.openF header ReadWriteMode 1024
                      journalHandle <- FH.openF journal ReadWriteMode 1024
                      retreivedMap <- readHeader headerHandle
                      let fJournal = Journal { journalID = filename
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
                            Just val -> Just $ FH.readBlock  (jHandle j)  val
                            Nothing -> Nothing

-- | Writes the header to the header file
writeHeader :: Journal -> IO ()
writeHeader j = do
                  let s = encode (oldBlocks j)
                  FH.writeAll (FH.handle $ hHandle j) s

-- | Reads the header information from the header file
readHeader :: FH.FHandle -> IO Map Integer Integer
readHeader fh = do
                  let val = FH.readAll fh
                  let decodedMap = decode (val)
                  return decodedMap


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


-- | Replay the data from the Journal to bring back the database into a consistent
-- state in case of a power failure
replayJournal = undefined

test = do 
    d <- FH.openF "abc.b" ReadWriteMode 1024   -- Truncates to zero length file 
    j <- newJournal d
    k <- writeToJournal j 256 (BSC.pack "Block number 256")
    l <- writeToJournal k 666 (BSC.pack "Block number 666")
    let x = Map.lookup 256 (oldBlocks l)
    let y = Map.lookup 666 (oldBlocks l)
    r1 <- readFromJournal l 256
    r2 <- readFromJournal l 666
    BS.putStrLn r1
    BS.putStrLn r2

