module Main where
 
import Data.Serialize.Packman

data Foo = A | B | C | D deriving Show

packAndPrint o = case pack o of
                   Left err -> putStrLn "Error"
                   Right _ -> putStrLn "Serialized!"

packAndUnpack o = case pack o of
                    Left err -> putStrLn "Error"
                    Right buf -> case unpack buf of
                      Left err -> putStrLn "Unpack error"
                      Right a  -> print a

main = do
  packAndPrint A
  packAndPrint B
  packAndUnpack C
  packAndUnpack 7
  packAndPrint (\x -> 5 + 5)
