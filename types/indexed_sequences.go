package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().Digest()
		return digest[:]
	})
}

func newIndexedMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt // chunk is written when the root sequence is written
			// TODO: This is no good, it's silly to be paging in all of these refs, let alone the type switch. sequenceChunker needs to know how to update numLeaves intelligently.
			child := mt.child
			if child == nil {
				child = ReadValue(mt.ChildRef(), cs)
			}
			switch child := child.(type) {
			case blobLeaf:
				numLeaves += child.Len()
			case listLeaf:
				numLeaves += child.Len()
			case metaSequence:
				numLeaves += child.numLeaves()
			default:
				panic("unsupported")
			}
		}

		meta := newMetaSequenceFromData(numLeaves, tuples, t, cs)
		return metaTuple{meta, ref.Ref{}, Uint64(tuples.uint64ValuesSum())}, meta
	}
}
