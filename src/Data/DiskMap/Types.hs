module Data.DiskMap.Types where

import Data.Hashable
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString as BS
import qualified  STMContainers.Map as Map
import Control.Concurrent.STM.TVar (TVar)
import Control.Exception.Base     (Exception)


-- |
data DiskMap k v = DiskMap
    MapConfig
    (STMMap k v)
    (TVar Bool)     -- Is read-only?

type STMMap k v = Map.Map k (MapItem v)

data MapItem v = Item {
    itemContent :: v
    ,needsDiskSync          :: Bool
    ,isBeingDeletedFromDisk :: Bool }
    deriving Show

data MapConfig = MapConfig FilePath


data CreateResult = Created | AlreadyExists deriving (Show, Eq)
data WriteException = PermissionDenied deriving (Show, Eq)
instance Exception WriteException

-- |
data MapItemResult k v a =
    -- Key + new item + user-defined return value type
    ItemUpdated k v a |
    -- Key + old item + user-defined return value type
    NotUpdated  k v a |
    NoSuchItem

getResult :: MapItemResult k v a -> Maybe a
getResult (ItemUpdated _ _ a) = Just a
getResult (NotUpdated  _ _ a) = Just a
getResult NoSuchItem          = Nothing

-- | Types that can be serialized and deserialized
class Serializable a where
    serialize   :: a -> BS.ByteString
    deserialize :: BS.ByteString -> Either String a

-- | Types that can be converted to a unique filename
class (Serializable k, Eq k, Hashable k) => ToFileName k where
    toFileName      :: k -> String
    fromFileName    :: String -> Either String k

    toFileName = C.unpack . B16.encode . serialize
    fromFileName = deserialize . fst . B16.decode . C.pack

type Success = Bool

-- Sync
data SyncOp = Sync | Delete


