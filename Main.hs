module Main where

import qualified Data.List as L
import qualified Data.Map as M
import RDBMS 

db = createTable (emptyDB) "Student" ["name", "email"]

initData :: [Values]
initData = [
           M.fromList [("name", "Tanaka"), ("email", "tanaka@gmail.com")]
           ,M.fromList [("name", "Satou"), ("email", "satou@yahoo.co.jp")]
           ]


main :: IO ()
main = do {
          putStrLn "Before insert data";
          putStrLn $ show $ runQuery db (select "Student" ["name"]);
          putStrLn $ show $ runQuery db (select "Student" ["email"]);
          putStrLn $ show $ runQuery db (select "Student" ["name", "email"]);
          putStrLn "After insert data";
          putStrLn $ show $ runQuery newDB (select "Student" ["name"]);
          putStrLn $ show $ runQuery newDB (select "Student" ["email"]);
          putStrLn $ show $ runQuery newDB (select "Student" ["name", "email"]);
          }
  where
    newDB = L.foldl' (\d v -> getDB $ runQuery d (insert "Student" v)) db initData
