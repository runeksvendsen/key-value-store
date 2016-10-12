module Data.DiskMap.Internal.Types where

import qualified  STMContainers.Map as Map
import Control.Concurrent.STM.TVar (TVar)


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

