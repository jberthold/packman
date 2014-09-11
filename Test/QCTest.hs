{-# LANGUAGE RecordWildCards, DeriveFunctor #-}
module QCTest(tests) where

import Distribution.TestSuite
import Test.QuickCheck

import Data.Traversable(Traversable, traverse, sequenceA)
import Data.Foldable hiding (foldl,foldr)
import qualified Data.Foldable as F
import Control.Applicative

import GHC.Packing

-- use "detailed" interface: defining test instances
tests :: IO [Test]
tests = mapM (return . Test . uncurry (runQC 10))
        [boldTrees, foldmap square (+) 0, foldmapforce square (+) 0 ]

square x = x*x

-- boldTrees :: (Arbitrary a, Eq a, Show a) => (String, a -> Property)
boldTrees :: (String, RoseTree Int -> Property)
boldTrees = ("bold trees", 
             \t -> ioProperty (do t' <- duplicate t
                                  return (t == t')))

foldmap f g x = ("foldmap",
                 \t -> ioProperty (do t' <- duplicate t
                                      return (fm t == fm t')))
    where fm tree = F.foldr g x (fmap f tree)

foldmapforce f g x = ("foldmapforce",
                      \t -> ioProperty (do let t'' = partEval t
                                           t' <- t'' `seq` duplicate t''
                                           return (fm t == fm t')))
    where fm tree = F.foldr g x (fmap f tree)

duplicate x = trySerialize x >>= deserialize

------------------------

runQC :: (Arbitrary a, Show a, Testable p) => 
         Int -> String -> (a -> p) -> TestInstance
runQC size n prop = TestInstance 
               { name = "QC test " ++ n
               , tags = [], options = []
               , setOption = \_ _ -> Right (runQC size n prop)
               , run = do r <- quickCheckWithResult stdArgs{maxSize=size} prop
                          return (Finished (readResult r))
               }
    where readResult :: Test.QuickCheck.Result -> Distribution.TestSuite.Result
          readResult Success{..} = Pass
          readResult GaveUp{..}  = Fail ("Insufficient amount of tests ("
                                         ++ show numTests ++ ")")
          readResult Failure{..} = Fail output
          readResult NoExpectedFailure{..} = Fail output

-- our test data. Uses a number of different constructors...
data RoseTree a 
    = Withered a
    | Rose1 a (RoseTree a)
    | Rose2 a (RoseTree a) (RoseTree a)
    | Rose3 a (RoseTree a) (RoseTree a) (RoseTree a)
    | Rose4 a (RoseTree a) (RoseTree a) (RoseTree a) (RoseTree a)
    -- finally, the normal one.
    | RoseN a [RoseTree a]
      deriving (Eq, Show, Read, Functor)

-- meaningless function to force parts of a tree
partEval :: RoseTree a -> RoseTree a
partEval (Withered x) = x `seq` Withered x
partEval (Rose2 x t u)  = partEval u `seq` x `seq` Rose2 x t u
partEval (Rose4 x t u v w)  = partEval u `seq` partEval w `seq` x 
                              `seq` Rose4 x t u v w
partEval t = t

instance Foldable RoseTree 
    where -- foldr :: (a -> b -> b) -> b -> Rosetree a -> b
          foldr f x (Withered a) = f a x
          foldr f x (Rose1 a t)  = f a (F.foldr f x t)
          foldr f x (Rose2 a t u)
              = -- f a (foldr (F.foldr f) x [t, u])
                f a (F.foldr f (F.foldr f x u) t)
          foldr f x (Rose3 a t u v)  = f a (foldl (F.foldr f) x [t,u,v])
          foldr f x (Rose4 a t u v w)  = f a (foldl (F.foldr f) x [t,u,v,w])
          foldr f x (RoseN a ts) = f a (foldl (F.foldr f) x ts)

instance Traversable RoseTree where
    -- traverse :: Applicative f => (a -> f b) -> RoseTree a -> f (RoseTree b)
    traverse f (Withered a) = Withered <$> f a
    traverse f (Rose1 a t)  = Rose1 <$> f a <*> traverse f t
    traverse f (Rose2 a t u) = Rose2 <$> f a <*> traverse f t <*> traverse f u
    traverse f (Rose3 a t u v) 
        = Rose3 <$> f a <*> traverse f t <*> traverse f u <*> traverse f v
    traverse f (Rose4 a t u v w) 
        = Rose4 <$> f a <*> traverse f t <*> traverse f u 
                        <*> traverse f v <*> traverse f w
    traverse f (RoseN a ts) = RoseN <$> f a <*> sequenceA (map (traverse f) ts)

instance Arbitrary a => Arbitrary (RoseTree a) where
    arbitrary = sized rt

rt :: Arbitrary a => Int -> Gen (RoseTree a)
rt 0 = arbitrary >>= return . Withered
rt n = oneof [ do t <- rt (n-1)
                  x <- arbitrary
                  return (Rose1 x t)
             , do t <- rt (n-1); u <- rt (n-1)
                  x <- arbitrary
                  return (Rose2 x t u) 
             , do t <- rt (n-1); u <- rt (n-1); v <- rt (n-1)
                  x <- arbitrary
                  return (Rose3 x t u v) 
             , do t <- rt (n-1); u <- rt (n-1); v <- rt (n-1); w <- rt (n-1)
                  x <- arbitrary
                  return (Rose4 x t u v w) 
             , do i <- choose (5,n)
                  ts <- sequence (replicate i (rt (n-1)))
                  x <- arbitrary
                  return (RoseN x ts) 
             ]

norm :: RoseTree a -> RoseTree a
norm (RoseN x []) = Withered x
norm (RoseN x [t]) = Rose1 x (norm t)
norm (RoseN x [t,u]) = Rose2 x (norm t) (norm u)
norm (RoseN x [t,u,v]) = Rose3 x (norm t) (norm u) (norm v)
norm (RoseN x [t,u,v,w]) = Rose4 x (norm t) (norm u) (norm v) (norm w)
norm t = t

