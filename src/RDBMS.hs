module RDBMS where

import Control.Monad.State 
import qualified Data.List as L
import qualified Data.Map as M
import qualified BTree as B
import qualified Data.Maybe as Maybe

newtype ColumnName = ColumnName String deriving (Show, Eq, Ord)

--一次索引は行の番号を弾くことができる
type PrimaryBIndex = B.BTree String Row
--二次索引はPrimary Keyを示すことで間接的に行を弾くことができる
type SecondaryBIndex = B.BTree String [String]

data ColumnOption = ColumnOption {
                  _default :: (Maybe (Maybe String))
                  ,_nullable :: Bool
                  } deriving (Show, Eq)

defaultOption :: ColumnOption 
defaultOption = ColumnOption Nothing True 

--Primary KeyはNot NULLでなければいけない,
--Not NULLならばdefault値としてNULLは指定できない
--default値を指定しないというのは可能（挿入時にユーザーが指定する必要がある)
validatePrimaryColumnOption :: ColumnOption -> Bool
validatePrimaryColumnOption c
  | (_nullable c) = False
  | otherwise = case (_default c) of
                  Just (Just s) -> True
                  Just Nothing -> False
                  Nothing -> True

--通常のカラムはNullableならば特に制約はない
--Not NULLならばdefault値としてNULLは指定できない
--default値を指定しないというのは可能（挿入時にユーザーが指定する必要がある)
validateNormalColumnOption :: ColumnOption -> Bool
validateNormalColumnOption c
  | (_nullable c) = True
  | otherwise = case (_default c) of
                  Just (Just s) -> True
                  Just Nothing -> False
                  Nothing -> True

validateColumnOption :: Column -> Bool
validateColumnOption c = if isPrimary c
                           then validatePrimaryColumnOption (_option c)
                           else validateNormalColumnOption (_option c)

fromColumnName :: ColumnName -> String
fromColumnName (ColumnName s) = s

--思い切ってPrimaryなカラムと通常のカラムを分けた
data Column = PColumn {
            _cname :: ColumnName
            ,_option :: ColumnOption
            ,_pbtree_index :: PrimaryBIndex
            }
            | NColumn {
                      _cname :: ColumnName
                      ,_option :: ColumnOption
                      ,_nbtree_index :: Maybe SecondaryBIndex
              } deriving (Show)

mkPColumn :: ColumnName -> ColumnOption -> Column
mkPColumn cname coption = if validatePrimaryColumnOption coption
                            then PColumn cname coption (B.empty 8)
                            else error "Invalid primary key option"

mkNColumn :: ColumnName -> ColumnOption -> Column
mkNColumn cname coption = if validateNormalColumnOption coption
                            then NColumn cname coption Nothing
                            else error "Invalid normal column option"


isPrimary :: Column -> Bool
isPrimary (PColumn _ _ _) = True
isPrimary _ = False

--カラム名が同じカラムは同じと扱う
instance Eq Column where
    a == b = (_cname a) == (_cname b)

type TableName = String
--最初はどのカラムもNullableということにしておく
type Row = M.Map ColumnName (Maybe String)

type Values = Row

data Table = Table (M.Map ColumnName Column) [Row] deriving (Show)

newtype DB = MkDB (M.Map TableName Table) deriving (Show)

emptyDB :: DB
emptyDB = (MkDB M.empty)

unDB :: DB -> M.Map TableName Table
unDB (MkDB ts) = ts

createTable :: DB -> TableName -> [Column] -> DB
createTable (MkDB ts) tname cs = if primaryExists
                                   then (MkDB (M.insert tname (Table columnMap []) ts))
                                   else error "One more primary key need"
  where
    --すべてのカラムが正しいことは保証されている
    primaryExists = L.any (\c -> isPrimary c) cs 
    columnMap = L.foldl' (\m c -> M.insert (_cname c) c m) M.empty cs

getColumn :: Table -> ColumnName -> Maybe Column
getColumn (Table cs _) cname = M.lookup cname cs

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

translateByValue :: (Ord a) => (a -> b -> Maybe c) -> M.Map a b -> M.Map a c
translateByValue f m = L.foldl' (\m (k,v) -> case f k v of
                                            Just v -> M.insert k v m
                                            Nothing -> m) M.empty (M.toList m)

fixValues :: Values -> [Column] -> Row
fixValues v cs = row
  where
    row :: Row
    row = L.foldl' (\r c -> 
                   let option = (_option c) in
                     case (M.lookup (_cname c) v) of
                       Just (Just s) -> M.insert (_cname c) (Just s) r
                       Just Nothing -> if (_nullable option)
                                         then M.insert (_cname c) Nothing r 
                                         else error ("Column " ++ (fromColumnName (_cname c)) ++ " can't be NULL")
                       Nothing -> case (_default option) of
                                    Just i -> M.insert (_cname c) i r
                                    Nothing -> if (_nullable option)
                                                 then M.insert (_cname c) Nothing r
                                                 else error ("Column '" ++ (fromColumnName (_cname c)) ++ "' doesn't have a default value")) M.empty cs

-- Insert into Table
insertIntoTable :: Table -> Values -> Table
insertIntoTable (Table cs rs) v = case alreadyExists of
                                    Just i -> error "Aleready exists"
                                    Nothing -> (Table newCS (rs ++ [insertRow]))
  where
    insertRow = fixValues v (M.elems cs)
    pKey = Maybe.fromJust $ L.find isPrimary (M.elems cs)
    alreadyExists = B.search (_pbtree_index pKey) (Maybe.fromJust (insertRow M.! (_cname pKey)))
    newCS = M.fromList $ L.map (\c -> if (isPrimary c) && (_cname c) == (_cname pKey)
                                        then ((_cname c), c {_pbtree_index = B.insert (_pbtree_index c) (Maybe.fromJust (insertRow M.! (_cname pKey))) insertRow})
                                        else ((_cname c), c)) (M.elems cs)

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

collectWhere' :: Table -> WhereClause -> [Row]
collectWhere' t@(Table cs _) (WAnd w1 w2) = (collectWhere' t w1) `L.intersect` (collectWhere' t w2)
collectWhere' t@(Table cs _) (WOr w1 w2)  = (collectWhere' t w1) `L.union` (collectWhere' t w2)
collectWhere' t@(Table cs rs) (WNot w1)  = L.filter (\r -> not (L.elem r matched)) rs
    where 
          matched = collectWhere' t w1

collectWhere' (Table cs rs) (WEq cname (Right s)) = case (M.lookup cname cs) of
                                                      Just c -> if (isPrimary c)
                                                                  then case s of
                                                                         Just s -> [Maybe.fromJust (B.search (_pbtree_index c) s)]
                                                                         Nothing -> []
                                                                  else L.filter (\r -> (r M.! cname) == s) rs
                                                      Nothing -> error ("Column '" ++ (fromColumnName cname) ++ "' doesn't exists.")

collectWhere' (Table cs rs) (WEq cname1 (Left cname2)) = case (M.lookup cname1 cs) of
                                                          Just c -> case (M.lookup cname2 cs) of
                                                                      Just c -> L.filter (\r -> (r M.! cname1) == (r M.! cname2)) rs
                                                                      Nothing -> error ("Column '" ++ (fromColumnName cname2) ++ "' doesn't exits.")
                                                          Nothing -> error ("Column '" ++ (fromColumnName cname1) ++ "' doesn't exits.")

collectWhere :: Table -> WhereClause -> [Row]
collectWhere = collectWhere'

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
