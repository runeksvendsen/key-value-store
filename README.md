# Software transaction memory-map with added persistence

An STM 'STMContainers.Map' which syncs each map key/item to a file on disk, from which the map can be restored later. Each key is stored as a file with the content being the serialized map item. So this is optimized for relatively large map items, where storing a file on disk for each item in the map is not an issue.

Optionally, writing to disk can be deferred, such that each map update doesn't touch the disk immediately, but instead only when the 'DiskSync' IO action, returned by 'newDiskMap', is evaluated.

This database is ACID-compliant, although the durability property is lost if deferred disk write is enabled.

The STM map provides a layer of atomicity, allowing multiple map items to be updated in a single atomic operation.
