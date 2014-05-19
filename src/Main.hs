{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GHCForeignImportPrim #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UnliftedFFITypes #-}

module Main where
 
import Control.Monad
import Unsafe.Coerce 

import GHC.Prim
import GHC.Types


data Foo = A | B | C | D

foreign import prim "stg_tryPack" tryPack# :: Any -> (# Int#, ByteArray# #)
pack :: a -> Int
pack obj = let (# err, buf #) = tryPack# (unsafeCoerce obj :: Any)
           in I# err

packAndPrint o = let x = pack o in print x

main = do
  packAndPrint A
  packAndPrint B
  packAndPrint C
  packAndPrint 7
  packAndPrint (\x -> 5 + 5)
