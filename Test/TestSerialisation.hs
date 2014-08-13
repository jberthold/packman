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
import System.Exit

import qualified Data.ByteString as B

import Control.Exception

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

fib :: Int -> Int 
fib x | x <= 1   = 1
      |otherwise = fib (x-1) + fib (x-2)

-- | duplicate data
duplicate :: a -> IO a
duplicate x = deserialize =<< trySerialize x 

-- | 
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
        putStrLn "Test program for serialisation"
        putStrLn "Evaluating some fib expressions before and after packing"
        let input  = [20..u]
	    flt    =  filter (> 10000) 
	    fibL   = (flt . map fib) input
-- [10946,17711,28657,46368,75025,121393,196418,317811,514229,832040,1346269]

        putStrLn "normal evaluation (take 3), should be \n[10946,17711,28657]"
	putStrLn (show $ take 3 fibL)

        testeval "packing one part" fibL (take 5) 
                     "[10946,17711,28657,46368,75025]"

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

