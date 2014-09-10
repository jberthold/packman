{-
  Some tests to verify that serialisation works as expected
-}
module AllTests(tests)
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
        { run = action >>= return . Finished . 
                           (\b -> if b then Pass 
                                  else Fail "unexpected output (see log)")
        , name = "Test case " ++ name
        , tags = []
        , options = []
        , setOption = \_ _ -> Right (runIt name action)
        }

tests :: IO [ Test ]
tests = do putStrLn "Running all tests"
           mapM (return . Test . uncurry runIt) mytests

-- all configured tests, see below
mytests = [eval_array, pack_array, pack_ThreadId, pack_MVar ]

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
                 return (out == "[0,2,4]")
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
                expectException P_CANNOTPACK $ trySerialize m
            )
