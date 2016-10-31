module Data.DiskMap.Internal.Helpers where

import Data.DiskMap.Types

import Control.Monad.STM
import qualified  STMContainers.Map as Map


-- | If a an item exists, read item i, replace in map with (f i), and return
-- | Just i. Else return Nothing. Note that the original, unmapped item is returned.
mapGetItem_Internal :: ToFileName k =>
    DiskMap k v
    -> (MapItem v -> MapItem v)
    -> k
    -> STM (Maybe (MapItem v))
mapGetItem_Internal (DiskMap _ m _ ) f k  = do
    maybeItem <- fetchItem m k
    case maybeItem of
        (Just item) ->
            Map.insert (f item) k m >> return maybeItem
        Nothing ->
            return Nothing

fetchItem :: ToFileName k =>
    STMMap k v -> k -> STM (Maybe (MapItem v))
fetchItem m k = do
    item <- Map.lookup k m
    case item of
        -- This means another thread is in the process of syncing the key/value to disk
        Just Item { needsDiskSync = True } -> retry
        -- This means another thread is in the process of deleting the key/value from disk
        Just Item { isBeingDeletedFromDisk = True } -> return Nothing
        _ -> return item

getItemNonAtomic :: ToFileName k =>
    DiskMap k v -> k -> STM (Maybe v)
getItemNonAtomic (DiskMap _ m _ ) k =
    fmap itemContent <$> fetchItem m k

updateItem :: (ToFileName k, Serializable v) => STMMap k v -> k -> v -> STM Bool
updateItem m k v = do
    maybeItem <- fetchItem m k
    case maybeItem of
        Just _ -> insertItem k v m >> return True
        Nothing -> return False

insertItem :: ToFileName k => k -> v -> STMMap k v -> STM ()
insertItem key item = Map.insert
    Item { itemContent = item, needsDiskSync = False, isBeingDeletedFromDisk = False } key

insertDiskSyncedChannel :: ToFileName k => k -> v -> STMMap k v -> STM ()
insertDiskSyncedChannel k item = Map.insert
    Item { itemContent = item, needsDiskSync = False, isBeingDeletedFromDisk = False } k

-- | Accumulate items from a list until the given function returns False
--  Examples:
--  > accumulateWhile (\accumulatedItems _ -> sum accumulatedItems < 25) [1,2,3,4,5,6,7,8,9]
--  > [1,2,3,4,5,6]
--  > accumulateSentence = accumulateWhile (\_ c -> c /= '.')
accumulateWhile :: ([a] -> a -> Bool) ->  [a] -> [a]
accumulateWhile f l = accumulateWhileImpl [] l f

accumulateWhileImpl :: [a] -> [a] -> ([a] -> a -> Bool) -> [a]
accumulateWhileImpl accumList [] _                      = accumList
accumulateWhileImpl accumList (x:xs) accumulateThis =
    if accumulateThis accumList x then
        accumulateWhileImpl (accumList ++ [x]) xs accumulateThis
    else
        accumList

