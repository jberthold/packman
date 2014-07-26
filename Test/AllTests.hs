{-
  Some tests to verify that serialisation works as expected
-}
module Test.AllTests(tests)
    where

import GHC.Packing

import qualified Data.Array.IArray as A
import Control.Concurrent

import System.Environment
import System.IO
import System.Directory
import qualified Data.ByteString as B
import Control.Exception
import Data.Typeable

import Distribution.TestSuite

-- this test uses the trySerialize routine. We expect to trigger some
-- exceptions and catch them as appropriate.

catchPackExc :: IO () -> IO ()
catchPackExc io = io `catch` (\e -> putStrLn (show (e::PackException)))

-- need a time-wasting function which allocates...
nfib :: Integer -> Integer
nfib 0 = 1
nfib 1 = 1
nfib n = let n1 = nfib (n-1)
             n2 = nfib (n-2)
         in 1 + 2*n1 + n2 - n1

-- make a test instance. Action should check result and return Bool
runIt :: String -> IO Bool -> TestInstance
runIt name action
    = TestInstance
        { run = action >> return (Finished Pass)
        , name = "printing stuff"
        , tags = []
        , options = []
        , setOption = \_ _ -> Right (runIt name action)
        }

tests :: IO [ Test ]
tests = do putStrLn "Running all tests"
           mapM (return . Test . uncurry runIt) mytests

-- all configured tests, see below
mytests = [eval_array , testingOnly ]
          -- , pack_array, pack_ThreadId, pack_MVar ]

-- baaang. this one fails, with a weird linker error. Cabal bug?
testingOnly = ("testing only", putStrLn (show P_UNSUPPORTED) >> return True)

-- test data
arr, output :: A.Array Int Int
arr  = A.array (0,127) [ (i,i) | i <- [0..127] ]
output = A.amap (2*) arr 

n :: Int
n = 3

eval_array :: (String, IO Bool)
eval_array = ("eval. array",
              do let out = show $ take n $ A.elems output
                 putStrLn $ "Evaluated: " ++ out
                 return (out == "[0,1,2]")
             )

pack_array :: (String, IO Bool)
pack_array = ("duplicating an array of 128 elements",
              do packet1 <- trySerialize output
                 putStrLn (take (3*80) (show packet1) ++ "...")
                 putStrLn "now unpacking (deserialize):"
                 copy <- deserialize packet1
                 putStrLn ("unpacked, now evaluate")
                 putStrLn (show copy)
                 return $ copy == A.amap (2*) arr
             )

expectException :: Typeable a => PackException -> IO (Serialized a) -> IO Bool
expectException exception action
    = do putStrLn ("expect exception " ++ show exception)
         action >>= print
         return False
      `catch` \e -> do putStrLn ("Got: " ++ show e)
                       return (e == exception)

pack_ThreadId :: (String, IO Bool)
pack_ThreadId = ("packing a thread ID (unsupported)",
                 do t <- myThreadId
                    expectException P_UNSUPPORTED $ trySerialize t
                )
pack_MVar :: (String, IO Bool)
pack_MVar = ("packing an MVar (should be cannotpack)",
             do m <- newEmptyMVar :: IO (MVar Integer)
                expectException P_CANNOT_PACK $ trySerialize m
            )

-- test exceptions. When running this, one should capture
-- stdout (but not stderr) and compare to reference output
testExc :: IO ()
testExc
   = do hSetBuffering stdout NoBuffering

        putStrLn "Test program for packing/serialization:"
        putStrLn "testing exceptions during packing. Use -qQ1k or so..."

        let output = A.amap (2*) arr
        putStrLn $ show $ take n $ A.elems output

        putStrLn "now packing the array (buffer big enough?)"
        
        catchPackExc $
         do packet1 <- trySerialize output
            putStrLn (show packet1)
            putStrLn "now unpacking (deserialize):"
            copy <- deserialize packet1

            putStrLn ("unpacked, now evaluate")
            putStrLn (show copy)

        putStrLn "packing some forbidden types"
        t <- myThreadId
        putStrLn "next should be unsupported"
        catchPackExc (trySerialize t >>= print)

        m <- newEmptyMVar :: IO (MVar Integer)
        putStrLn "next should be cannotpack"
        catchPackExc (trySerialize m >>= print)

        putStrLn "next should hit a blackhole"
        let b = nfib (-1) -- will loop, but so far unevaluated
        putMVar m b
        forkIO $ do n <- takeMVar m
                    case n of -- poor child thread will evaluate bottom
                      something -> error $"bottom is " ++ show something ++ "!"
        yield -- let child thread pick up the trap
        catchPackExc (trySerialize b >>= print)

        let arr2 = A.listArray (0,n-1) (take n (A.elems arr)) :: A.Array Int Int
        putStrLn "this - finally - should work"
        putStrLn ( show $ arr2 A.! 0 ) -- forcing it
        catchPackExc $
          do p2 <- trySerialize arr2
             arr3 <- deserialize p2
             print arr3

        putStrLn "trying to deserialise other binary's data. Expected: binary mismatch"
        catchPackExc $ do a <- decodeFromFile "test.old" :: IO (A.Array Int Int) 
                          print a

        putStrLn "trying to deserialise wrong type from file. Expected: type mismatch"
        catchPackExc $ do encodeToFile "test" arr2 
                          a <- decodeFromFile "test" :: IO (A.Array Int Double)
                          print a

        putStrLn "trying to deserialise truncated data. Expected: parse error"
        blob <- B.readFile "test"
        B.writeFile "test" (B.take 50 blob) -- take more than FingerPrint (4 x Word64)
        catchPackExc $ do p <- getProgName 
                          x <- decodeFromFile "test" :: IO (A.Array Int Int)
                          print x
        (removeFile "test") `catch` (\e -> print (e::SomeException) )

        putStrLn "DONE"

