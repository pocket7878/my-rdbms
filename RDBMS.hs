module RDBMS where

import Control.Monad.State 
import qualified Data.List as L
import qualified Data.Map as M

type ColumnName = String
type TableName = String
--最初はどのカラムもNullableということにしておく
type Row = M.Map ColumnName (Maybe String)
--投入データは明示的にNullを指定しなくて良い。無いもんはない
type Values = M.Map ColumnName String

data Table = Table [ColumnName] [Row] deriving (Show)

newtype DB = MkDB (M.Map TableName Table) deriving (Show)

emptyDB :: DB
emptyDB = (MkDB M.empty)

unDB :: DB -> M.Map TableName Table
unDB (MkDB ts) = ts

createTable :: DB -> TableName -> [ColumnName] -> DB
createTable (MkDB ts) tname cs = (MkDB (M.insert tname (Table cs []) ts))

-- Mapの形式を整える
standardizeMap :: (Ord a) => M.Map a b -> [a] -> M.Map a (Maybe b)
standardizeMap m standard = newMap
  where
    tmpMap = M.filterWithKey (\k v -> k `elem` standard) m
    newMap = L.foldl' (\m s ->  M.insert s (M.lookup s tmpMap) m) M.empty standard

-- Insert into Table
insertIntoTable :: Table -> Values -> Table
insertIntoTable (Table cs rs) v = (Table cs (insertRow : rs))
  where
    insertRow = standardizeMap v cs

selectFromTable :: Table -> [ColumnName] -> [Row]
selectFromTable (Table cs rs) selectCs = results
  where
    newSelectCs = L.filter (\c -> c `elem` cs) selectCs
    results = L.map (\r -> M.filterWithKey (\k v -> k `elem` newSelectCs) r) rs

data Query = SelectQuery TableName [ColumnName] | InsertQuery TableName Values

select :: TableName -> [ColumnName] -> Query
select t cs = SelectQuery t cs

insert :: TableName -> Values -> Query
insert t v = InsertQuery t v

data QueryResults = SelectResults [Row] | Success | Fail deriving (Show)

runQuery :: Query -> State DB QueryResults
runQuery (SelectQuery tname scs) = do 
                                      db <- get
                                      let table = M.lookup tname (unDB db) in 
                                          case table of
                                            Just t -> return (SelectResults (selectFromTable t scs))
                                            Nothing -> return Fail

runQuery (InsertQuery tname v) = do
                                    db <- get
                                    case table db of
                                      Just _ ->  put (newDB db) >> return Success
                                      Nothing -> return Fail
  where
    table db = M.lookup tname (unDB db)
    newDB db = (MkDB (M.adjust (\t -> insertIntoTable t v) tname (unDB db)))
