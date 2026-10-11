// Package persist is Sylphy's durability layer.
//
// A data directory holds a MANIFEST (the commit point), at most one base
// snapshot (base-NNNNNN.snap) and zero or more append-only segments
// (seg-NNNNNN.aof). State on disk is the base plus the segments in manifest
// order. Segments are plain RESP arrays, one command per record, so existing
// tools can read them.
//
// This file set is the foundation: a file-system seam (FS) with a real and a
// fault-injecting in-memory implementation, the manifest codec, the AOF record
// codec and the snapshot codec. Everything on disk is checksummed with CRC-32C
// except AOF records, which are plain RESP and are validated by parsing.
//
// Decoders never panic and never trust a length field: memory use follows the
// bytes actually present, not the sizes the input claims.
package persist
