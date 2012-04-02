{-# LANGUAGE FlexibleInstances #-}
import System.HaskDB.Backend 
import Test.QuickCheck
import Control.Monad
import qualified Data.Map as M 
import Test.HUnit 
import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

instance Arbitrary Mode  where 
    arbitrary = oneof $ map return [Write , ReadWrite , Read]

instance (Ord a, Arbitrary a,Arbitrary b) => Arbitrary (M.Map a b) where 
    arbitrary =  liftM M.fromList (listOf1 arbitrary)

instance Arbitrary a => Arbitrary (File a) where 
    arbitrary = liftM2 File arbitrary arbitrary

instance Arbitrary a => Arbitrary (TestDisk a) where 
    arbitrary = do 
        b <- arbitrary :: Arbitrary b => Gen (M.Map FilePath (File b))
        let list = M.toList b
        len <- choose (1,length list)
        subset <- takeSubset len list 
        size <- arbitrary :: Gen Int 
        op <- openFiles (map fst subset) M.empty 
        return $ TestDisk b b size  op
      where 
        takeSubset :: Int -> [a] -> Gen [a]
        takeSubset 0 list = return list 
        takeSubset n list = do 
            t <- oneof $ map return list 
            rest <- takeSubset (n-1) list 
            return (t:rest)
        openFiles :: [FilePath] -> M.Map FilePath Mode -> Gen (M.Map FilePath Mode)
        openFiles [] f = return f
        openFiles (x:xs) f = do 
            md <- arbitrary :: Gen Mode 
            openFiles xs (M.insert x md f)


-- closing two times a file should be same 
prop_double_close :: TestDisk String -> String -> Bool 
prop_double_close t s = (closeFile (closeFile t s) s)  == closeFile t s

-- writing and then reading the same block should return same value 
prop_readwrite :: TestDisk String -> String -> Int -> String -> Bool 
prop_readwrite t fp bn val = readB (writeB (openFile t fp ReadWrite) fp bn val) fp bn buffers == Just val

-- after syncing block should be on disk
prop_sync :: TestDisk String -> String -> Int -> String -> Bool 
prop_sync t fp bn val = readB (flushBuffer (writeB (openFile t fp ReadWrite) fp bn val) fp) fp bn disk == Just val

-- closing a file automatically syncs 
prop_close :: TestDisk String -> String -> Int -> String -> Bool 
prop_close t fp bn val = readB (closeFile (writeB (openFile t fp ReadWrite) fp bn val) fp) fp bn disk == Just val

main = defaultMain tests
tests = [
        testGroup "Sorting Group 1" [
                testProperty "double close " prop_double_close,
                testProperty "read and write " prop_readwrite,
                testProperty "sync" prop_sync,
                testProperty "close" prop_close
            ]]

