module Data.DiskMap.Internal.Update where

import Data.DiskMap.Types
import Data.DiskMap.Internal.Helpers (mapGetItem_Internal)
import Data.DiskMap.Sync.Sync (writeEntryToFile, singleStateDeleteDisk)

import Data.Maybe (isJust)
import qualified Control.Monad.STM as STM
import Control.Monad.STM (STM)
import Control.Monad (forM_, void)
import qualified  STMContainers.Map as Map
import Control.Concurrent.STM.TVar (readTVar)
import Control.Exception.Base     (throwIO, finally)


-- | Retrieve map item and simultaneously mark the retrieved item for disk deletion
markAsBeingDeleted :: ToFileName k => DiskMap k v -> k -> STM (Maybe (MapItem v))
markAsBeingDeleted m k =
    mapGetItem_Internal m
        (\i@Item {} -> i { isBeingDeletedFromDisk = True }) k

guardReadOnly :: DiskMap k v -> IO (DiskMap k v)
guardReadOnly dm@(DiskMap (MapConfig _) _ readOnlyTVar) = do
    readOnly <- STM.atomically $ readTVar readOnlyTVar
    if not readOnly then
            return dm
        else
            throwIO PermissionDenied

-- |Update items in the map by supplying an STM action which returns a
--  'MapItemResult' for that item, which tells us whether or not disk sync is needed.
--  Once the map has been initialized, this should be the only function that updates item contents
updateMapItem :: (ToFileName k, Serializable v) =>
    DiskMap k v -> STM [MapItemResult k v a] -> IO [MapItemResult k v a]
updateMapItem dm action =
    guardReadOnly dm >>=
        flip updateMapItem_IgnoreReadOnly action

-- |
updateMapItem_IgnoreReadOnly :: (ToFileName k, Serializable v) =>
    DiskMap k v -> STM [MapItemResult k v a] -> IO [MapItemResult k v a]
updateMapItem_IgnoreReadOnly dm@(DiskMap (MapConfig dir) m _) updateActions = do
    updateResults <- STM.atomically $ do
        resList <- updateActions
        -- Mark all affected items as needing disk sync
        forM_ resList (itemSetNeedsDiskSync dm)
        return resList
    forM_ updateResults syncToDisk
    return updateResults
    where syncToDisk res =
            case res of
                i@(ItemUpdated key val _) -> void $
                    writeEntryToFile dir key val `finally`
                        STM.atomically (removeNeedsDiskSync m i)    --TODO: retry on exception?
                _ -> return ()

-- |
_deleteMapItem :: (ToFileName k, Serializable v) =>
    DiskMap k v -> k -> IO Success
_deleteMapItem dm@(DiskMap (MapConfig syncDir) m _) key = do
    exists <- fmap isJust $ STM.atomically $
        markAsBeingDeleted dm key
    if not exists then
            return False
        else do
            singleStateDeleteDisk syncDir key
            STM.atomically $ Map.delete key m
            return True


itemSetNeedsDiskSync :: (ToFileName k, Serializable v) =>
    DiskMap k v -> MapItemResult k v a -> STM (MapItemResult k v a)
itemSetNeedsDiskSync m miRes = do
    case miRes of
        res@(ItemUpdated k _ _) ->
            -- Will retry if needsDiskSync == True already
            mapGetItem_Internal m markForSync k >>=
            maybe
                (error "BUG: Tried to mark non-existing item as needing disk sync")
                (const $ return res)
        whatever -> return whatever
    where
        markForSync i = i { needsDiskSync = True }

removeNeedsDiskSync :: (ToFileName k, Serializable v) =>
    STMMap k v -> MapItemResult k v a -> STM (MapItemResult k v a)
removeNeedsDiskSync m miRes = do
    case miRes of
        res@(ItemUpdated k _ _) -> do
            item <- Map.lookup k m
            case item of
                Just i  -> do
                    Map.insert (i { needsDiskSync = False }) k m
                    return res
                Nothing -> error "BUG: Tried to mark non-existing item as no longer needing disk sync"
        whatever -> return whatever


