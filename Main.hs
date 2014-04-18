module Main where

import Control.Monad.State
import qualified Data.List as L
import qualified Data.Map as M
import RDBMS 

db :: DB
db = createTable (emptyDB) "Student" ["name", "email"]

main :: IO ()
main =  print $ runState (do {
                      runQuery (insert "Student" (M.fromList [("name", "Satou"), ("email", "hoge")]));
                      runQuery (insert "Student" (M.fromList [("name", "Tanaka"), ("email", "moge")]));
                      results <- (runQuery (select "Student" ["name"]));
                      return results;
                      }) db
