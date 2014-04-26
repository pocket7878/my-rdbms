module Main where

import Control.Monad.State
import qualified Data.List as L
import qualified Data.Map as M
import qualified BTree as B
import RDBMS 

db :: DB
db = createTable (emptyDB) "Student" [ColumnName "name", ColumnName "email", ColumnName "sub-email"]

query = (select "Student" [ColumnName "name", ColumnName "email"])

root = B.empty 4

inserts :: (Ord a, Ord b) => B.BTree a b -> [(a, b)] -> B.BTree a b
inserts root xs = L.foldl' (\t (k, v) -> B.insert t k v) root xs

ntree = inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4), ("e", 5), ("f", 6), ("g", 7)]

main :: IO ()
--main =  do 
--           evalStateT (do {
--                          insert "Student" (M.fromList [(ColumnName "name", "Satou"), (ColumnName "email", "hoge")]);
--                          insert "Student" (M.fromList [(ColumnName "name", "Tanaka"), (ColumnName "email", "moge")]);
--                          insert "Student" (M.fromList [(ColumnName "name", "Yamamoto"), (ColumnName "email", "yama"), (ColumnName "sub-email", "yama")]);
--                          liftIO $ putStrLn "Before update";
--                          results <- select "Student" [ColumnName "name"] Nothing;
--                          liftIO $ print results;
--                          update "Student" [SetToValue (ColumnName "name") (Just "Hoge")] Nothing;
--                          liftIO $ putStrLn "After update";
--                          results <- select "Student" [ColumnName "name"] Nothing;
--                          liftIO $ print results;
--                          return ()
--                          }) db
main = do
    print $ inserts root [("a", 1)]
    print $ inserts root [("a", 1), ("b", 2), ("c", 3)]
    print $ inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4)]
    print $ inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4), ("e", 5)]
    print $ inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4), ("e", 5), ("f", 6)]
    print $ inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4), ("e", 5), ("f", 6), ("g", 7)]
    print $ B.search ntree "a"
    print $ B.search ntree "c"
    print $ B.search ntree "g"
