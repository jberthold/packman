{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GHCForeignImportPrim #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE UnliftedFFITypes #-}

module Data.Serialize.Packman (
    Serialized,
    pack,
    unpack
  ) where
 
import Control.Monad
import Unsafe.Coerce 

import GHC.Prim
import GHC.Types
import Data.Primitive


data Serialized a = Serialized ByteArray#

data SerializeError
  = Blackhole
  | NoBuffer
  | CannotPack
  | Unsupported
  | Impossible
  | Garbled


fromErrorCode :: Int# -> SerializeError
fromErrorCode 1# = Blackhole
fromErrorCode 2# = NoBuffer    
fromErrorCode 3# = CannotPack
fromErrorCode 4# = Unsupported
fromErrorCode 5# = Impossible
fromErrorCode 6# = Garbled


foreign import prim "stg_tryPack" tryPack# :: Any -> (# Int#, ByteArray# #)

pack :: a -> Either SerializeError (Serialized a)
pack obj = let (# err, buf #) = tryPack# (unsafeCoerce obj :: Any)
           in case err of
             0# -> Right (Serialized buf)
             i  -> Left (fromErrorCode i)


foreign import prim "stg_unpack" unpack# :: ByteArray# -> (# Int#, a #)

unpack :: Serialized a -> Either SerializeError a
unpack (Serialized buf) =
  let (# err, obj #) = unpack# buf
  in case err of
    0# -> Right obj
    i  -> Left (fromErrorCode i)

