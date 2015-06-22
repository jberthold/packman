-- testing serialisation with multiple concurrent threads

import GHC.Packing

import qualified Data.Array.IArray as A
import Control.Concurrent

import System.Environment
import System.IO
import System.Directory
import System.Exit

import qualified Data.ByteString as B

import Control.Exception

-- need a time-wasting function which allocates...
nfib :: Integer -> Integer
nfib 0 = 1
nfib 1 = 1
nfib n = let n1 = nfib (n-1)
             n2 = nfib (n-2)
         in 1 + 2*n1 + n2 - n1

fib :: Int -> Int 
fib x | x <= 1   = 1
      |otherwise = fib (x-1) + fib (x-2)

-- | duplicate data
duplicate :: a -> IO a
duplicate x = (deserialize =<< trySerialize x)

testeval :: (Show b) => String -> a -> (a -> b) -> String -> IO ()
testeval name dat f expected
    = do putStr name
         dat' <- duplicate dat
         let out = show (f dat')
         if (out /= expected)
         then do putStrLn (": FAILED\t unexpected " ++ out) 
                 error (name ++ " failed")
         else putStrLn (": OK\t" ++ out)

main
   = do hSetBuffering stdout NoBuffering

        args <- getArgs
        let u = if (null args) then 40 else read (head args)
            n = if length args < 2 then 10 else read (args!!1)

        putStrLn "Multithreaded test program for serialisation"
        let input  = [20..u]
            flt    =  filter (> 10000) 
            fibL   = (flt . map fib) input
-- [10946,17711,28657,46368,75025,121393,196418,317811,514229,832040,1346269]

        putStrLn "normal evaluation (take 3), should be \n[10946,17711,28657]"
        putStrLn (show $ take 3 fibL)

        let doThread i = 
                do v <- newEmptyMVar
                   -- note that all threads use fibL. Many calls to
                   -- trySerialize will block on blackholes (the core
                   -- operation will block and retry)
                   forkIO (do testeval (show i)
                                       (cycle fibL) (!!i)
                                       (show (fibL!!(i `mod` length fibL)))
                              putMVar v ())
                   return v
        vs <- mapM doThread [0..n-1]

        putStrLn "forked, waiting"
        mapM_ takeMVar vs
        putStrLn "done"
{-

        let f2 x y = fib (x+y)
        testeval "packing a function (2 arg.s)" f2 
                     (\f -> (take 3 . flt . map (f 0)) input)
                     "[10946,17711,28657]"

        let f3 x a b c y d = head $ map (map fib) 
                             [[(x+a+b+c+d)-const .. (y+a+b+c+d)-const ]]
            const  = head fibL - 1
            f3'    = f3 (head fibL) 0 0 
        const `seq` f3' `seq` 
         testeval "packing a list function (2 arg.s, 1 supplied)"
                  f3' (\f -> take 3 (flt (f 0 (u+const) 0)))
                  "[10946,17711,28657]"

        putStrLn "DONE"

        let n    = max 10 u
            size = 3*u -- default 120
            arr :: A.Array Int Int
            arr  = A.array (0,size-1)
                   [ (i,i) | i <- [0..size-1] ]
            output = A.amap (2*) arr

        -- output some of the original elements
        putStrLn $ show $ take n $ A.elems arr

        testeval "packing an unevaluated array"
                 output (take n . A.elems)
                 (show (take n [0,2..]))

-}
