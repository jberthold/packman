{-
  Some tests to 
-}
-- module TestSerialisation(tests)
--    where

import GHC.Packing

import qualified Data.Array.IArray as A
import Control.Concurrent

import System.Environment
import System.IO
import System.Directory

import qualified Data.ByteString as B

import qualified Control.Exception as E

-- this test uses the trySerialize routine. We expect to trigger some
-- exceptions and catch them as appropriate.

catchPackExc :: IO () -> IO ()
catchPackExc io = io `E.catch` (\e -> putStrLn (show (e::PackException)))

-- need a time-wasting function which allocates...
nfib :: Integer -> Integer
nfib 0 = 1
nfib 1 = 1
nfib n = let n1 = nfib (n-1)
             n2 = nfib (n-2)
         in 1 + 2*n1 + n2 - n1

-- test exceptions. When running this, one should capture
-- stdout (but not stderr) and compare to reference output
--testExc :: IO ()
--testExc
main
   = do hSetBuffering stdout NoBuffering

        putStrLn "Test program for packing/serialization:"

        let n    = 1   -- if (length args < 2) then 1 else read (args!!1)
            size = 128 -- if null args then 128 else read (head args)::Int
            arr :: A.Array Int Int
            arr  = A.array (0,size-1) 
                   [ (i,i) | i <- [0..size-1] ]

        let output = A.amap (2*) arr
        putStrLn $ show $ take n $ A.elems output

        putStrLn "now packing the array (buffer big enough?)"
        
        catchPackExc $
         do packet1 <- trySerialize output
            -- putStrLn (show packet1)
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
        catchPackExc $ do a <- decodeFromFile "pack.old"
                          print (a::A.Array Int Int)

        putStrLn "trying to deserialise wrong type from file. Expected: type mismatch"
        catchPackExc $ do encodeToFile "pack" arr2 
                          a <- decodeFromFile "pack"
                          print (a::A.Array Int Double)

        putStrLn "trying to deserialise truncated data. Expected: parse error"
        blob <- B.readFile "pack"
        B.writeFile "pack" (B.take 50 blob) -- take more than FingerPrint (4 x Word64)
        catchPackExc $ do p <- getProgName 
                          x <- decodeFromFile "pack" :: IO (A.Array Int Int)
                          print x
        (removeFile "pack") `E.catch` (\e -> print (e::E.SomeException) )

        putStrLn "DONE"

