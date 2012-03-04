-- A Journal is temporary file to keep track of data yet to be written to the database.
-- Only one Journal per transaction?
-- Journal is a collection of "diffs" in a particular transaction
module Journal where

import System.Directory (removeFile)
import Data.ByteString as BS
import qualified FileHandling as FH

-- Do we need to distinguish between database handle and journal handle?
type JHandle = FH.FHandle

data Journal = Journal { jHandle :: FH.FHandle -- Handle for the Journal file
                       , dHandle :: FH.FHandle -- Handle for the database file
                       , jDiffs :: [Diff]      -- List of diffs
                       }

-- | A Diff is the "old data" that is to be replaced by the writeblock succeeding it
data Diff = Diff { diffId :: Int
                 , blockNumber :: Int
                 , blockData :: BS
                 }


-- | Find existing Journals present on the disk 
findJournals :: IO [FilePath]
findJournals = undefined


-- | Open a Journal and maybe do something with it
openJournal :: FilePath -> IO Journal
openJournal fp = do
                    jh <- openF fp WriteMode 1024
                    dh <- openF "abc.db" WriteMode 1024
                    let FJournal = Journal { jHandle = jh
                                           , dHandle = dh
                                           , jDiffs = readDiffsFrom jh 
                                           }

--how to store the diffs in a journal
readDiffsFrom :: FH.FHandle -> [Diff]
readDiffsFrom jh = undefined

-- | Remove the Journal after successful write to the database
-- | Whether to zero out the file or remove it?
removeJournal :: Journal -> IO ()
removeJournal j = do
                    filetoRemove <- jHandle j
                    -- get the FilePath of the Journal
                    removeFile fp


-- | Restore the data from the Journal to bring back the database into a consistent state
restoreJournal = undefined

