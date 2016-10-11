module Data.DiskMap.Internal.Update where

import Data.DiskMap.Types
import Data.DiskMap.Internal.Helpers (mapGetItem_Internal)
import Data.DiskMap.Sync.Sync (writeEntryToFile, singleStateDeleteDisk)

import Data.Maybe (isJust)
import Control.Monad.STM
import Control.Monad (forM, forM_)
import qualified  STMContainers.Map as Map
import Control.Concurrent.STM.TVar (readTVar)
import Control.Exception.Base     (throwIO)


-- | Retrieve map item and simultaneously mark the retrieved item for disk deletion
markAsBeingDeleted :: ToFileName k => DiskMap k v -> k -> STM (Maybe (MapItem v))
markAsBeingDeleted m k =
    mapGetItem_Internal m
        (\i@Item {} -> i { isBeingDeletedFromDisk = True }) k


guardReadOnly :: DiskMap k v -> IO (DiskMap k v)
guardReadOnly dm@(DiskMap (MapConfig _) _ readOnlyTVar) = do
    readOnly <- atomically $ readTVar readOnlyTVar
    if not readOnly then
            return dm
        else
            throwIO PermissionDenied

updateMapItem dm action =
    guardReadOnly dm >>=
        flip _updateMapItem action

-- |Once the map has been initialized, this should be the only function that updates item contents
_updateMapItem :: (ToFileName k, Serializable v) =>
    DiskMap k v -> STM [MapItemResult k v a] -> IO [MapItemResult k v a]
_updateMapItem (DiskMap (MapConfig dir) m readOnlyTVar) updateActions = do
    readOnly <- atomically $ readTVar readOnlyTVar
    if not readOnly then do
            updateResults <- atomically $ do
                resList <- updateActions
                forM_ resList (setNeedsDiskSync m True)
                return resList
            let syncToDisk res =
                    case res of
                        i@(ItemUpdated key val _) -> do
                            writeEntryToFile dir key val
                            atomically $ setNeedsDiskSync m False i
                            return ()
                        _ -> return ()
            forM updateResults syncToDisk
            return updateResults
        else
            throwIO PermissionDenied

-- |Not finished yet
_deleteMapItem :: (ToFileName k, Serializable v) =>
    DiskMap k v -> k -> IO Success
_deleteMapItem (DiskMap (MapConfig syncDir) m _) key = do
    exists <- fmap isJust $ atomically $ do
        maybeItem <- Map.lookup key m
        Map.delete key m
        return maybeItem
    if not exists then
            return False
        else do
            singleStateDeleteDisk syncDir key
            return True


setNeedsDiskSync :: (ToFileName k, Serializable v) =>
    STMMap k v -> Bool -> MapItemResult k v a -> STM (MapItemResult k v a)
setNeedsDiskSync m needsSync miRes = do
    case miRes of
        res@(ItemUpdated k _ _) -> do
            item <- Map.lookup k m
            case item of
                Just i  -> do
                    Map.insert (i { needsDiskSync = needsSync }) k m
                    return res
                Nothing -> error "BUG: 'ItemUpdated' result but key not present"
        whatever -> return whatever

