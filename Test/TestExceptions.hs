{-
  Some tests to verify that serialisation works as expected
-}
module Main(mytests, main)
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

-- import Distribution.TestSuite
import System.Exit
import Control.Monad

-- this test uses the trySerialize routine. We expect to trigger some
-- exceptions and catch them as appropriate.

catchPackExc :: IO () -> IO ()
catchPackExc io = io `catch` (\e -> putStrLn (show (e::PackException)))

expectException :: (Show a) => PackException -> IO a -> IO Bool
expectException exception action
    = do putStrLn ("expect exception " ++ show exception)
         action >>= print
         return False
      `catch` \e -> do putStrLn ("Got: " ++ show e)
                       return (e == exception)

main :: IO ()
main = do putStrLn "Running all tests"
          args <- getArgs
          let n    = if (length args < 2) then 3 else read (args!!1)
              size = if null args then 128 else read (head args)::Int
          -- test data from arguments, avoiding over-optimisation
              arr, arr2, output :: A.Array Int Int
              arr  = A.array (0,127) [ (i,i) | i <- [0..127] ]
              output = A.amap (2*) arr 
              arr2 = A.listArray (0,2*n-1) (take (2*n) (A.elems arr))
              cfg  = (output, arr2, n)
          (mapM_ (runIt cfg) mytests) `finally` (rmv "test") 

type Config = (A.Array Int Int, A.Array Int Int, Int)

type MyTest = Config -> (String, IO Bool)

-- run a defined test 
runIt :: Config -> MyTest -> IO ()
runIt cfg f
    = do putStrLn $ "Test: " ++ name ++ ": "
         result <- action
         putStrLn $ "Result: " ++ show result
         if not result then error "test failed!" else return ()
    where (name, action) = f cfg

-- all configured tests, see below
mytests :: [ MyTest ]
mytests = [ evalArray,  packArray, packThreadId, packMVar, packBH,
            unpackOther, unpackWrongType, unpackTruncated, unpackGarbled ]

evalArray :: MyTest
evalArray (output, _, n)
    = ("eval. array",
       do let out = show $ take n $ A.elems output
          putStrLn $ "Evaluated: " ++ out
          return (out == "[0,2,4]"))

packArray :: MyTest
packArray (output, arr2, n)
    = ("duplicating an array of 128 elements",
              do packet1 <- trySerialize output
                 putStrLn (take (3*80) (show packet1) ++ "...")
                 putStrLn "now unpacking (deserialize):"
                 copy <- deserialize packet1
                 putStrLn ("unpacked, now evaluate")
                 putStrLn (show copy)
                 return $
                    take (2*n) (A.elems copy) == A.elems (A.amap (2*) arr2)
             )

packThreadId :: MyTest
packThreadId _ = ("packing a thread ID (unsupported)",
                  do t <- myThreadId
                     expectException P_UNSUPPORTED $ trySerialize t
                 )
packMVar :: MyTest
packMVar _ = ("packing an MVar (should be cannotpack)",
              do m <- newEmptyMVar :: IO (MVar Integer)
                 expectException P_CANNOT_PACK $ trySerialize m
             )

packBH :: MyTest
packBH _ = ("should hit a blackhole",
            do let b = nfib 38 -- (-1) -- will loop, but so far unevaluated
               m <- newEmptyMVar
               putMVar m b
               child <- forkIO $
                       do n <- takeMVar m
                          case n of -- poor child thread will evaluate bottom
                            some -> error $"bottom is " ++ show some ++ "!"
               yield -- let child thread pick up the trap
               expectException P_BLACKHOLE
                              (trySerialize b)
                               -- `finally` (killThread child)
         )

unpackOther :: MyTest
unpackOther _ = ("deserialise other binary's data (binary mismatch)",
                 expectException P_BinaryMismatch
                 (decodeFromFile "pack.old" :: IO (A.Array Int Int))
                )

unpackWrongType :: MyTest
unpackWrongType  (output, arr2, n)
    = ("deserialise wrong type from file (type mismatch)",
                   do encodeToFile "test" arr2
                      expectException P_TypeMismatch
                       (decodeFromFile "test" :: IO (A.Array Int Double))
                  )

unpackTruncated :: MyTest
unpackTruncated  (output, arr2, n)
    = ("deserialise truncated data. Expected: parse error",
       do encodeToFile "test" arr2
          blob <- B.readFile "test"
          B.writeFile "test" (B.take 50 blob)
               -- take more than FingerPrint (4 x Word64)
          expectException P_ParseError
              (decodeFromFile "test" :: IO (A.Array Int Int))
      )

unpackGarbled :: MyTest
unpackGarbled (output, arr2, n)
    = ("deserialise garbled data. Expected: garbled data",
       do encodeToFile "test" arr2
          blob <- B.readFile "test"
          B.writeFile "test" (tamperWith blob)
          expectException P_GARBLED
           (decodeFromFile "test" :: IO (A.Array Int Int))
      )

tamperWith :: B.ByteString -> B.ByteString
tamperWith b = B.concat [b1, B.pack [11,11], B.drop 2 b2]
    where n       = B.length b
          (b1,b2) = B.splitAt (n - 10) b
-- do not touch the FingerPrints (4 x Word64)

-- file cleanup, catching all exceptions
rmv :: FilePath -> IO ()
rmv f = (removeFile f) `catch` (\e -> print (e::SomeException))


-- need a time-wasting function which allocates...
nfib :: Integer -> Integer
nfib 0 = 1
nfib 1 = 1
nfib n = let n1 = nfib (n-1)
             n2 = nfib (n-2)
         in 1 + 2*n1 + n2 - n1
