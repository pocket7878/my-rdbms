module Main where

import Control.Monad.State
import qualified Data.List as L
import qualified Data.Map as M
import qualified BTree as B
import RDBMS 

nameOption = ColumnOption Nothing False 
emailOption = ColumnOption (Just (Just "unknown-email")) True 

db :: DB
db = createTable (emptyDB) "Student" [mkPColumn (ColumnName "name") nameOption,
                                     mkNColumn (ColumnName "email") emailOption,
                                     mkNColumn (ColumnName "sub-email") defaultOption]

query = (select "Student" [ColumnName "name", ColumnName "email"])

root = B.empty 4

inserts :: (Ord a, Ord b) => B.BTree a b -> [(a, b)] -> B.BTree a b
inserts root xs = L.foldl' (\t (k, v) -> B.insert t k v) root xs

ntree = inserts root [("a", 1), ("b", 2), ("d", 3), ("c", 4), ("e", 5), ("f", 6), ("g", 7)]

main :: IO ()
main =  do 
           evalStateT (do {
                          insert "Student" (M.fromList [(ColumnName "name", Just "Satou"), (ColumnName "email", Just "hoge")]);
                          insert "Student" (M.fromList [(ColumnName "name", Just "Takeru"), (ColumnName "email", Just "unnamed-user-email")]);
                          insert "Student" (M.fromList [(ColumnName "name", Just "Tanaka"), (ColumnName "email", Just "moge")]);
                          insert "Student" (M.fromList [(ColumnName "name", Just "Yamamoto"), (ColumnName "email", Just "yama"), (ColumnName "sub-email", Just "yama")]);
                          liftIO $ putStrLn "Before update";
                          results <- select "Student" [ColumnName "name"] Nothing;
                          liftIO $ print results;
                          update "Student" [SetToValue (ColumnName "name") (Just "Hoge")] Nothing;
                          liftIO $ putStrLn "After update";
                          results <- select "Student" [ColumnName "name"] Nothing;
                          liftIO $ print results;
                          return ()
                          }) db
