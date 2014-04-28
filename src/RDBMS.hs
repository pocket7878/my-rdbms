module RDBMS where

import Control.Monad.State 
import qualified Data.List as L
import qualified Data.Map as M

newtype ColumnName = ColumnName String deriving (Show, Eq, Ord)

data ColumnOption = ColumnOption {
                  _default :: (Maybe String)
                  ,_nullable :: Bool
                  ,_primary :: Bool}
                  deriving (Show, Eq)

defaultOption :: ColumnOption 
defaultOption = ColumnOption Nothing True False

--Nullableでないカラムにはデフォルト値としてNullは指定できない
validateColumnOption :: ColumnOption -> Bool
validateColumnOption c = case _default c of
                           Just _ -> True
                           Nothing -> not (_nullable c)

fromColumnName :: ColumnName -> String
fromColumnName (ColumnName s) = s

type Column = M.Map ColumnName ColumnOption

type TableName = String
--最初はどのカラムもNullableということにしておく
type Row = M.Map ColumnName (Maybe String)

type Values = Row

data Table = Table Column [Row] deriving (Show)

newtype DB = MkDB (M.Map TableName Table) deriving (Show)

emptyDB :: DB
emptyDB = (MkDB M.empty)

unDB :: DB -> M.Map TableName Table
unDB (MkDB ts) = ts

createTable :: DB -> TableName -> Column -> DB
createTable (MkDB ts) tname cs = (MkDB (M.insert tname (Table cs []) ts))

-- Mapの形式を整える
standardizeMap :: (Ord a) => M.Map a b -> [a] -> M.Map a (Maybe b)
standardizeMap m standard = newMap
  where
    tmpMap = M.filterWithKey (\k v -> k `elem` standard) m
    newMap = L.foldl' (\m s ->  M.insert s (M.lookup s tmpMap) m) M.empty standard

translateByKey :: (Ord a) => (a -> Maybe c) -> M.Map a b -> M.Map a c
translateByKey f m = L.foldl' (\m k -> case f k of
                                            Just v -> M.insert k v m
                                            Nothing -> m) M.empty (M.keys m)

fixValues :: Values -> Column -> Row
fixValues v c = row
  where
    row = translateByKey (\k -> let option = (c M.! k) in
                                  case (M.lookup k v) of
                                    Just (Just s) -> (Just (Just s))
                                    Just Nothing -> if (_nullable option)
                                                      then Just Nothing
                                                      else error ("Column " ++ (fromColumnName k) ++ " can't be NULL")
                                    Nothing -> Just (_default option)) c

-- Insert into Table
insertIntoTable :: Table -> Values -> Table
insertIntoTable (Table cs rs) v = (Table cs (insertRow : rs))
  where
    insertRow = fixValues v cs

data QueryResults = SelectResults [Row] | Success | Fail deriving (Show)
data SetTo = SetToValue ColumnName (Maybe String) | SetToColumn ColumnName ColumnName
data WhereClause = WAnd WhereClause WhereClause | WOr WhereClause WhereClause | WNot WhereClause | WEq ColumnName (Either ColumnName (Maybe String))

applySetTo :: Row -> SetTo -> Row
applySetTo r (SetToValue cname s) = M.adjust (\_ -> s) cname r
applySetTo r (SetToColumn place val) = M.adjust (\_ -> r M.! val) place r

applyWhere :: Row -> WhereClause -> Bool
applyWhere r (WAnd w1 w2) = applyWhere r w1 && applyWhere r w2
applyWhere r (WOr w1 w2) = applyWhere r w1 || applyWhere r w2
applyWhere r (WNot w1) = not $ applyWhere r w1
applyWhere r (WEq cname (Right ms)) = rowData == ms
    where
      rowData = r M.! cname
applyWhere r (WEq cname1 (Left cname2)) = rdata1 == rdata2
    where
      rdata1 = r M.! cname1
      rdata2 = r M.! cname2 

filterRow :: [Row] -> Maybe WhereClause -> [Row]
filterRow rs w = case w of
                    Just wc -> L.filter (\r -> applyWhere r wc) rs
                    Nothing -> rs

selectFromTable :: Table -> [ColumnName] -> Maybe WhereClause -> [Row]
selectFromTable (Table cs rs) selectCs w = results
  where
    newSelectCs = L.filter (\c -> M.member c cs) selectCs
    results = L.map (\r -> M.filterWithKey (\k v -> k `elem` newSelectCs) r) $ filterRow rs w

updateTable :: Table -> [SetTo] -> Maybe WhereClause -> Table
updateTable (Table cs rs) sets Nothing   = (Table cs (L.map (\r -> L.foldl' (\r s -> applySetTo r s) r sets) rs))
updateTable (Table cs rs) sets (Just w)  = (Table cs (L.map (\r -> if (applyWhere r w) 
                                                                     then L.foldl' (\r s -> applySetTo r s) r sets 
                                                                     else r) rs))


data Query = SelectQuery TableName [ColumnName] (Maybe WhereClause) 
           | InsertQuery TableName Values  
           | UpdateQuery TableName [SetTo] (Maybe WhereClause)

runQuery :: Query -> StateT DB IO QueryResults
runQuery (SelectQuery tname scs wc) = do 
                                        db <- get
                                        let table = M.lookup tname (unDB db) in 
                                            case table of
                                              Just t -> return (SelectResults (selectFromTable t scs wc))
                                              Nothing -> return Fail

runQuery (InsertQuery tname v) = do
                                    db <- get
                                    case table db of
                                      Just _ ->  put (newDB db) >> return Success
                                      Nothing -> return Fail
  where
    table db = M.lookup tname (unDB db)
    newDB db = (MkDB (M.adjust (\t -> insertIntoTable t v) tname (unDB db)))

runQuery (UpdateQuery tname sets w) = do
                                        db <- get
                                        case M.lookup tname (unDB db) of
                                          Just t -> put (newDB db) >> return Success
                                          Nothing -> return Fail
  where
    newDB db = (MkDB (M.adjust (\t -> updateTable t sets w) tname (unDB db)))

select :: TableName -> [ColumnName] -> Maybe WhereClause -> StateT DB IO QueryResults
select t cs w = runQuery (SelectQuery t cs w)

insert :: TableName -> Values -> StateT DB IO QueryResults
insert t v = runQuery (InsertQuery t v)

update :: TableName -> [SetTo] -> Maybe WhereClause -> StateT DB IO QueryResults
update t s w = runQuery (UpdateQuery t s w)
