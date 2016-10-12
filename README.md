# Simple, multi-threaded key/value store
##### Software Transactional Memory-map with added persistence

### WARNING
Experimental software - I use it mostly in development, as a simple, fast, local key-value store.

`stm-diskmap` is an [STMContainers.Map](https://www.stackage.org/haddock/lts-7.3/stm-containers-0.2.15/STMContainers-Map.html) which syncs each map key/value to a file on disk, from which the map can be restored later. Each key/value is stored as a file, with file name being the hex-encoded key and the file content the serialized value. So this is optimized for relatively large map items, where storing a single file on disk for each item in the map is not an issue (trillions of items would probably be too much).

This database is ACID-compliant (assuming no bugs). The STM map provides a layer of atomicity, allowing multiple map items to be updated in a single atomic operation.

