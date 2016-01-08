package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func isSequenceOrderedByIndexedType(t Type) bool {
	return t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
}

// Given a leaf in an ordered sequence, returns the values in that leaf which define the ordering of the sequence.
type getLeafOrderedValuesFn func(Value) []Value

// Returns a cursor to |key| in |ms|, plus the leaf + index that |key| is in. |t| is the type of the ordered values.
func findLeafInOrderedSequence(ms metaSequence, t Type, key Value, getValues getLeafOrderedValuesFn, cs chunks.ChunkStore) (cursor *sequenceCursor, leaf Value, idx int) {
	cursor, leaf = newMetaSequenceCursor(ms, cs)

	if isSequenceOrderedByIndexedType(t) {
		orderedKey := key.(OrderedValue)

		cursor.seekBinary(func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(OrderedValue).Less(orderedKey)
		})
	} else {
		cursor.seekBinary(func(mt sequenceItem) bool {
			return !mt.(metaTuple).value.(Ref).TargetRef().Less(key.Ref())
		})
	}

	if current := cursor.current().(metaTuple); current.ChildRef() != valueFromType(cs, leaf, leaf.Type()).Ref() {
		leaf = readMetaTupleValue(current, cs)
	}

	if leafData := getValues(leaf); isSequenceOrderedByIndexedType(t) {
		orderedKey := key.(OrderedValue)

		idx = sort.Search(len(leafData), func(i int) bool {
			return !leafData[i].(OrderedValue).Less(orderedKey)
		})
	} else {
		idx = sort.Search(len(leafData), func(i int) bool {
			return !leafData[i].Ref().Less(key.Ref())
		})
	}

	return
}

func newOrderedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(orderedSequenceWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().Digest()
		return digest[:]
	})
}

func newOrderedMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
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
			case setLeaf:
				numLeaves += child.Len()
			case mapLeaf:
				numLeaves += child.Len()
			case metaSequence:
				numLeaves += child.numLeaves()
			default:
				panic("unsupported")
			}
		}

		meta := newMetaSequenceFromData(numLeaves, tuples, t, cs)
		return metaTuple{meta, ref.Ref{}, tuples.last().value}, meta
	}
}
