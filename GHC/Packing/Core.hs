{-# LANGUAGE CPP, MagicHash, UnboxedTuples #-}
{-# LANGUAGE GHCForeignImportPrim, ForeignFunctionInterface, 
             UnliftedFFITypes #-}

{-| 

Module      : GHC.Packing
Copyright   : (c) Jost Berthold, 2010-2015,
License     : BSD3
Maintainer  : jost.berthold@gmail.com
Stability   : experimental
Portability : no (depends on GHC internals)

= Wrapper module for the foreign primitive operations

-}

module GHC.Packing.Core
    ( trySerialize, trySerializeWith, deserialize
    ) where

import GHC.Packing.Type
import GHC.Packing.PackException

import GHC.Exts
import GHC.Prim
import Control.Monad.Primitive
import Data.Primitive.ByteArray

import Control.Exception(throw)

-- the entire package won't support GHC < 7.8
#if __GLASGOW_HASKELL__ < 708
#error This module assumes GHC-7.8 or above
#endif

-- | Non-blocking serialisation routine using 'PackException's to
-- signal errors. This version does not block the calling thread when
-- a black hole is found, but instead signals the condition by the
-- 'P_BLACKHOLE' exception.
trySerialize :: a -> IO (Serialized a) -- throws PackException (RTS)
trySerialize x = trySerializeWith x defaultBufSize

-- | default buffer size used by trySerialize
defaultBufSize :: Int
defaultBufSize = 10 * 2^20 -- 10 MB

-- | Extended interface function: Allocates a buffer of given size (in
-- bytes), serialises data into it, then truncates the buffer to the
-- required size before returning it (as @'Serialized' a@)
trySerializeWith :: a -> Int -> IO (Serialized a) -- using instance PrimMonad IO
trySerializeWith dat bufsize
    = do buf <- newByteArray bufsize
         size <- trySerializeInto buf dat
         buf' <- truncate' buf size
         ByteArray b# <- unsafeFreezeByteArray buf'
         return (Serialized { packetData = b# })

-- | core routine. Packs x into mutable byte array buf, returns size
-- of packed x in buf
trySerializeInto :: MutableByteArray RealWorld -> a -> IO Int
trySerializeInto (MutableByteArray buf# ) x 
    = primitive (tryPack (unsafeCoerce# x :: Any) buf# )

-- | calls primitive, decodes/throws errors + wraps Int# size into Int
tryPack :: Any -> MutableByteArray# s
        -> State# s -> (# State# s , Int #)
tryPack x# buf# s = case tryPack# x# buf# s of
                      (# s', 0#, size# #) -> (# s', I# size# #)
                      (# s', e#,   0#  #) 
                          | isBHExc e# -> repack s'
                          | otherwise  -> (# s', throw (decodeEx e#) #)
    where -- packing blocked, eval the blocking closure that we found
          -- (i.e. block on it) and re-pack afterwards. The first
          -- StgWord of the ByteArray contains the address (written by
          -- the packing routine, see BLACKHOLE case in packClosure).
      repack s = case readAddrArray# buf# 0# s of
                   (# s', bh #) -> case (addrToAny# bh) of -- or seq it?
                                     _ -> tryPack x# buf# s'

-- | serialisation primitive, implemented in C. Returns: a
-- status/error code and size used inside the array
foreign import prim "stg_tryPack" tryPack#
    :: Any -> MutableByteArray# s -> State# s -> (# State# s, Int#, Int# #)

-- GHC-7.8 does not have an in-place shrink operation for MutableByteArrays
-- (added in GHC-7.9 on August 16, 2014)
-- GHC-7.9, August 2014 :: MutableByteArray# s -> Int# -> State# s -> State# s
-- with this one available, tryPack could do the work
-- for GHC-7.8, we copy
truncate' :: PrimMonad m => MutableByteArray (PrimState m) -> Int -> m (MutableByteArray (PrimState m))
truncate' b size 
    = if sizeofMutableByteArray b < size
      then throw P_NOBUFFER -- XXX other error?
      else do b' <- newByteArray size
              copyMutableByteArray b' 0 b 0 size
              return b'

--------------------------------------------------------

-- | Deserialisation function. May throw @'PackException'@ @'P_GARBLED'@
deserialize :: Serialized a -> IO a
deserialize p = primitive (deser (packetData p))

deser :: ByteArray# -> State# s -> (# State# s, a #)
deser buf s = case unpack# buf s of
                (# s', 0#, x #) -> (# s', x #)
                (# s', n#, _ #) -> (# s', throw (decodeEx n#) #)

foreign import prim "stg_unpack" unpack# :: ByteArray# -> State# s -> (# State# s, Int#, a #)

