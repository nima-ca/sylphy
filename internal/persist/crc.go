package persist

import "hash/crc32"

// castagnoli is the CRC-32C table behind every checksum on disk. It is
// read-only after initialization.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)
