module Main where
 
import GHC.Packing
import Control.Exception

data Foo = A | B | C | D deriving Show

packAndPrint o = trySerialize o >> putStrLn "Serialized"

packAndUnpack o = trySerialize o >>= deserialize >>= print

main = do
  packAndPrint A
  packAndPrint B
  packAndUnpack C
  packAndUnpack 7
  packAndPrint (\x -> 5 + 5)
