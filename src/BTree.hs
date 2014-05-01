{-# LANGUAGE RankNTypes #-}
module BTree(
            BTree
            ,empty
            ,singleton
            ,search
            ,insert
            ,update
            ,toList
            ,keys
            ,elems
            ) where

import qualified Data.List as L

data BTree a b = LeafNode {
               _keys :: [a]
               ,_values :: [b]
               ,_limit :: Int
               ,_current :: Int
               }
               | InteriorNode {
                              _keys :: [a]
                              ,_trees :: [BTree a b]
                              ,_limit :: Int
                              ,_current :: Int
                              } 
               deriving (Show)

empty :: forall a b. Int -> BTree a b
empty limit = LeafNode [] [] limit 0

singleton :: forall a b. Int -> a -> b -> BTree a b
singleton limit k v = LeafNode [k] [v] limit 1

isLeaf :: BTree a b -> Bool
isLeaf (LeafNode _ _ _ _) = True
isLeaf _ = False

search :: (Ord a) => BTree a b -> a -> Maybe b
search t k 
  | isLeaf t = case L.elemIndex k (_keys t) of
                 Just i -> Just $ (_values t) L.!! i
                 Nothing -> Nothing
  | k < (head (_keys t)) = search (head (_trees t)) k
  | k > (last (_keys t)) = search (last (_trees t)) k
  | otherwise = case matchKey of
                  Just i -> search ((_trees t) L.!! (i + 1)) k
                  Nothing -> Nothing
    where
      matchKey = L.elemIndex k (_keys t)

insertToLeaf :: (Ord a) => BTree a b -> a -> b -> BTree a b
insertToLeaf t k v = t {_keys = newKeys, _values = newVals, _current = (_current t) + 1}
  where
    newKeyValuePair = L.sortBy (\(k1, _) (k2, _) -> compare k1 k2) $ zip (k : (_keys t)) (v : (_values t))
    (newKeys, newVals) = unzip newKeyValuePair

divideLeaf :: (Ord a) => BTree a b -> a -> b -> (a, BTree a b, BTree a b)
divideLeaf t k v = (newKey
                   ,LeafNode leftKey leftVal (_limit t) (length leftKey)
                   ,LeafNode rightKey rightVal (_limit t) (length rightKey))
                   where
                     newKeyValuePair = L.sortBy (\(k1, _) (k2, _) -> compare k1 k2) $ zip (k:(_keys t)) (v:(_values t))
                     leftKeyValuePair = take ((_limit t) `div` 2) newKeyValuePair
                     rightKeyValuePair = drop ((_limit t) `div` 2) newKeyValuePair
                     newKey = fst $ head $ rightKeyValuePair
                     (leftKey, leftVal) = unzip leftKeyValuePair
                     (rightKey, rightVal) = unzip rightKeyValuePair

replaceListAtBy :: (a -> a) -> [a] -> Int -> [a]
replaceListAtBy f ls idx = (take idx ls) ++ [f (ls L.!! idx)] ++ (drop (idx + 1) ls)

replaceListAt :: [a] -> Int -> a -> [a]
replaceListAt ls idx item = replaceListAtBy (const item) ls idx

insertAtIndex :: [a] -> Int -> a -> [a]
insertAtIndex ls idx item = (take idx ls) ++ [item] ++ (drop idx ls)

insertToInterior :: (Ord a) => BTree a b -> a -> BTree a b -> BTree a b -> Either (BTree a b) (a, BTree a b, BTree a b)
insertToInterior t k l r
  | (_limit t) == (_current t) = Right (centerKey, leftTree, rightTree)
  | otherwise = Left t {_keys = newKeys, _trees = newVals, _current = (_current t) + 1}
  where
    insertIndex = L.findIndex (\x -> k < x) (_keys t)
    newKeys = case insertIndex of
                Just i -> insertAtIndex (_keys t) i k
                Nothing -> (_keys t) ++ [k]
    newVals = case insertIndex of
                Just i -> insertAtIndex (replaceListAt (_trees t) i l) i r
                Nothing -> (init (_trees t)) ++ [l, r]
    centerIdx = (length newKeys) `div` 2
    centerKey = newKeys L.!! centerIdx
    leftKeys = take centerIdx newKeys
    leftVals = take (centerIdx + 1) newVals
    rightKeys = drop (centerIdx + 1) newKeys
    rightVals = drop (centerIdx + 1) newVals
    leftTree = InteriorNode leftKeys leftVals (_limit t) (length leftKeys)
    rightTree = InteriorNode rightKeys rightVals (_limit t) (length rightKeys)


insert' :: (Ord a) => BTree a b -> a -> b -> Either (BTree a b) (a, BTree a b, BTree a b)
insert' t k v 
  | isLeaf t && (_limit t) == (_current t) = Right (divideLeaf t k v)
  | isLeaf t = Left (insertToLeaf t k v)
  | k < (head (_keys t)) = case insert' (head (_trees t)) k v of
                             Right (newKey, l, r) -> insertToInterior t newKey l r
                             Left newT -> Left t {_trees = (newT : (tail (_trees t)))}
  | k > (last (_keys t)) = case insert' (last (_trees t)) k v of
                             Right (newKey, l , r) -> insertToInterior t newKey l r
                             Left newT -> Left t {_trees = ((init (_trees t)) ++ [newT])}
  | otherwise = case matchKey of
                  Just i -> case insert' ((_trees t) L.!! i) k v of
                              Right (newKey, l, r) -> insertToInterior t newKey l r
                              Left newT -> Left t {_trees = replaceListAt (_trees t) i newT}
                  Nothing -> error "Illigal"
    where
      matchKey = L.elemIndex k (_keys t)

insert :: (Ord a) => BTree a b -> a -> b -> BTree a b
insert root k v = case insert' root k v of
                    Right (newKey, l, r) -> InteriorNode [newKey] [l, r] (_limit root) 1
                    Left t -> t


updateBy :: (Ord a) => (b -> b) -> BTree a b -> a -> BTree a b
updateBy f t k
  | isLeaf t = case L.elemIndex k (_keys t) of
                 Just i -> t {_values = replaceListAtBy f (_values t) i}
                 Nothing -> t
  | k < (head (_keys t)) = let newTree = updateBy f (head (_trees t)) k in
                            t {_trees = (newTree : (tail (_trees t)))}
  | k > (last (_keys t)) = let newTree = updateBy f (last (_trees t)) k in
                            t {_trees = ((init (_trees t)) ++ [newTree])}
  | otherwise = case matchKey of
                  Just i -> let newTree = updateBy f ((_trees t) L.!! (i + 1)) k in
                              t {_trees = (replaceListAt (_trees t) (i + 1) newTree)}
                  Nothing -> error "Illigal"
    where
      matchKey = L.elemIndex k (_keys t)

update :: (Ord a) => BTree a b -> a -> b -> BTree a b
update t k v = updateBy (const v) t k


toList :: BTree a b -> [(a, b)]
toList (LeafNode keys values _ _) = zip keys values
toList (InteriorNode _ ts _ _) = concat $ L.map (\t -> toList t) ts

keys t = L.map fst $ toList t
elems t = L.map snd $ toList t
