{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | 

Module      : GHC.Packing
Copyright   : (c) Jost Berthold, 2010-2014,
License     : BSD3
Maintainer  : jb.diku@gmail.com
Stability   : experimental
Portability : no (depends on GHC internals)

= Serialisation of Haskell data structures (independent of evaluation)

Haskell heap structures can be serialised, capturing their current
state of evaluation, and deserialised later during the same program
run (effectively duplicating the data). Serialised data can also be
written to storage or sent over a network, and deserialised in a
different run or different instance of the /same/ executable binary.

The feature can be used to implement message passing over a network
(which is where the runtime support originated), or for various
applications based on data persistence, for instance checkpointing and
memoisation.

The library described here supports an operation to serialise Haskell
heap data:

> trySerialize :: a -> IO (Serialized a)

The routine will throw a 'PackException' if an error occurs inside the
C code which accesses the Haskell heap (see @'PackException'@).
In presence of concurrent threads, another thread might be evaluating
data /referred to/ by the data to be serialised.  It would be nice to
/block/ the calling thread in this case, but this is not possible in
the library version (see <#background Background Information> below).
'trySerialize' variant will instead signal the condition as
'PackException' 'P_BLACKHOLE'.

The inverse operation to serialisation is

> deserialize :: Serialized a -> IO a

The data type 'Serialized' a includes a phantom type @a@ to ensure
type safety within one and the same program run. Type @a@ can be
polymorphic (at compile time, that is) when 'Serialized' @a@ is not used
apart from being argument to 'deserialize'.

The @Show@, @Read@, and @Binary@ instances of @Serialized a@ require an
additional 'Typeable' context (which requires @a@ to be monomorphic)
in order to implement dynamic type checks when parsing and deserialising
data from external sources.
Consequently, the 'PackException' type contains exceptions which indicate
parse errors and type/binary mismatch.

-}

module GHC.Packing
    ( -- * Serialisation Operations
      trySerialize, trySerializeWith
    , deserialize

      -- * Data Types and instances
    , Serialized
      -- $ShowReadBinary
    , PackException(..)
      -- $packexceptions

      -- * Serialisation and binary file I/O
    , encodeToFile 
    , decodeFromFile

    -- * Background Information
      -- $primitives
   )
    where

-- all essentials are defined in other modules, and reexported here
import GHC.Packing.PackException
import GHC.Packing.Type
import GHC.Packing.Core

import Data.Binary
import Control.Exception
import Data.Typeable


-- | Write serialised binary data directly to a file. May throw 'PackException's.
encodeToFile :: Typeable a => FilePath -> a -> IO ()
encodeToFile path x = trySerialize x >>= encodeFile path

-- | Directly read binary serialised data from a file. May throw
-- 'PackException's (catches I/O and Binary exceptions from decoding
-- the file and re-throws 'P_ParseError')
decodeFromFile :: Typeable a => FilePath -> IO a
decodeFromFile path = do ser <- (decodeFile path) 
                                  `catch` 
                                  (\(e::ErrorCall) -> throw P_ParseError)
                         deserialize ser -- exceptions here go through

----------------------------------------
-- digressive documentation

{- $ShowReadBinary

The power of evaluation-orthogonal serialisation is that one can
/externalise/ partially evaluated data (containing thunks), for
instance write it to disk or send it over a network.

Therefore, the module defines a 'Data.Binary' instance for
'Serialized' a, as well as instances for 'Read' and 'Show'@ which
satisfy @ 'read' . 'show' == 'id' :: 'Serialized' a -> 'Serialized' a@.

The phantom type is enough to ensure type-correctness when serialised
data remain in one single program run. However, when data from
previous runs are read in from an external source, their type needs to
be checked at runtime. Type information must be stored together with
the (binary) serialisation data.

The serialised data contain pointers to static data in the generating
program (top-level functions and constants) and very likely to
additional library code. Therefore, the /exact same binary/ must be
used when reading in serialised data from an external source. A hash
of the executable is therefore included in the representation as well.

-}

{- $packexceptions

'PackException's can occur at Haskell level or in the foreign primop.
The Haskell-level exceptions all occur when reading in
'GHC.Packing.Serialised' data, and are:

* 'P_BinaryMismatch': the serialised data have been produced by a
different executable (must be the same binary).
* 'P_TypeMismatch': the serialised data have the wrong type
* 'P_ParseError': serialised data could not be parsed (from binary or
text format)

The other exceptions are return codes of the foreign primitive
operation, and indicate errors at the C level. Most of them occur when
serialising data; the exception is 'P_GARBLED' which indicates corrupt
serialised data.

-}

{- $primitives

  #background#

The functionality exposed by this module builds on serialisation of
Haskell heap graph structures, first implemented in the context of
implementing the GpH implementation GUM (Graph reduction on a 
Unified Memory System) and later adopted by the implementation of
Eden. Independent of its evaluation state, data and thunks can be
transferred between the (independent) heaps of several running Haskell
runtime system instances which execute the same executable.

The idea to expose the heap data serialisation functionality 
(often called /packing/) to Haskell by itself was first described in 
 Jost Berthold. /Orthogonal Serialisation for Haskell/.
 In Jurriaan Hage and Marco Morazan, editors, 
 /IFL'10, 22nd Symposium on Implementation and Application of 
 Functional Languages/, Springer LNCS 6647, pages 38-53, 2011.
This paper can be found at 
<http://www.mathematik.uni-marburg.de/~eden/papers/mainIFL10-withCopyright.pdf>,
the original publication is available at 
<http://www.springerlink.com/content/78642611n7623551/>.

The core runtime support consists of just two operations:
(slightly paraphrasing the way in which GHC implements the IO monad here)

> serialize#   :: a -> IO ByteArray# -- OUTDATED, see below
> deserialize# :: ByteArray# -> IO a -- which is actually pure from a mathematical POW

However, these operations are completely unsafe with respect to Haskell
types, and may fail at runtime for various other reasons as well. 
Type safety can be established by a phantom type, but needs to be checked
at runtime when the resulting data structure is externalised (for instance,
saved to a file). Besides prohibiting unprotected type casts, another
restriction that needs to be explicitly checked in this case is that 
different programs cannot exchange data by this serialisation. When data are
serialised during execution, they can only be deserialised by exactly the 
same executable binary because they contain code pointers that will change
even by recompilation.

Other failures can occur because of the runtime system's limitations, 
and because some mutable data types are not allowed to be serialised.
A newer API therefore suggests additions towards exception handling
and better usability.
The original primitive @'serialize'@ is modified and now returns error
codes, leading to the following type (again paraphrasing):

> serialize# :: a -> IO ( Int# , ByteArray# )

where the @Int#@ encodes potential error conditions returned by the runtime.

A second primitive operation has been defined, which considers the presence
of concurrent evaluations of the serialised data by other threads:

> trySerialize# :: a -> IO ( Int# , ByteArray# )

Further to returning error codes, this primitive operation will not block
the calling thread when the serialisation encounters a blackhole in the
heap. While blocking is a perfectly acceptable behaviour (making packing
behave analogous to evaluation wrt. concurrency), the @'trySerialize'@
variant allows one to explicitly control it and avoid becoming unresponsive.

as well, and differs from @trySerialize@ in that
it blocks the calling thread when a blackhole is found during serialisation.

The Haskell layer and its types protect the interface function @'deserialize'@
from being applied to  grossly wrong data (by checking a fingerprint of the 
executable and the expected type), but deserialisation is fragile by nature
(unpacking code pointers and data).
The primitive operation in the runtime system will only detect grossly wrong
formats, and the primitive will return error code @'P_GARBLED'@ when data
corruption is detected.

> deserialize# :: ByteArray# -> IO ( Int# , a )
-}
