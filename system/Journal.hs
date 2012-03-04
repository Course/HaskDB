-- | A Journal is temporary file to keep track of data soon to be modified in the database.
-- Only one Journal per transaction?
-- Journal is a collection of "old blocks" in a particular transaction
module Journal where
import System.IO
import System.Random
import Data.ByteString as BS
import qualified FileHandling as FH 

-- | Do we need to distinguish between database handle and journal handle?
type JHandle = FH.FHandle
type OldBlock = Int

data Journal = Journal { jPath :: FilePath -- FilePath of the journal file
                       , hHandle :: FH.FHandle -- Handle for the header file
                       , jHandle :: FH.FHandle -- Handle for the Journal file
                       , dHandle :: FH.FHandle -- Handle for the database File
                       , oldBlocks :: [OldBlock]      -- List of oldBlocks
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
                  headerHandle <- FH.openF header WriteMode 1024
                  journalHandle <- FH.openF journal WriteMode 1024
                  let fJournal = Journal { jPath = header
                                         , hHandle = headerHandle
                                         , jHandle = journalHandle
                                         , dHandle = dh
                                         , oldBlocks = []
                                         }
                  return fJournal

-- | How to store old blocks in a journal
readOldBlocksFrom :: Journal -> IO [BS.ByteString]
readOldBlocksFrom j = do
                        let len = Prelude.length (oldBlocks j) 
                        Prelude.mapM (readFromJournal j)  [0..len - 1]

readFromJournal :: Journal -> Integer -> IO BS.ByteString
readFromJournal j bn = FH.readBlock  (jHandle j)  bn

-- | Write to a journal given block number and blockData
writeToJournal :: Journal -> Int -> BS.ByteString -> IO ()
writeToJournal j bn bd = do
                           FH.appendBlock  (jHandle j) bd -- write the block to actual file
                           (oldBlocks j) ++ [bn]

-- | To zero out the journal file
resetJournal :: Journal -> IO ()
resetJournal = FH.truncateF . FH.filePath . jHandle


-- | Replay the data from the Journal to bring back the database into a consistent
-- state in case of a power failure
replayJournal = undefined

