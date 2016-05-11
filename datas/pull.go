package datas

import (
	"fmt"

	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

// CopyMissingChunksP copies to |sink| all chunks in source that are reachable from (and including) |sourceRef|, skipping chunks that |sink| already has
// TODO: this needs to be tested.
func CopyMissingChunksP(source, sink Database, sourceRef, sinkRef types.Ref, concurrency int) {
	missing, _ := types.ChunksDiff(source, sink, sourceRef, sinkRef)
	fmt.Println("missing", len(missing), "chunks")
	for _, r := range missing {
		sink.batchStore().SchedulePut(source.batchStore().Get(r.TargetRef()), types.Hints{})
	}
	sink.batchStore().Flush()
}

// CopyReachableChunksP copies to |sink| all chunks reachable from (and including) |r|, but that are not in the subtree rooted at |exclude|
func CopyReachableChunksP(source, sink Database, sourceRef, sinkRef types.Ref, concurrency int) {
	missing, moar := types.ChunksDiff(source, sink, sourceRef, sinkRef)
	fmt.Println("missing", len(missing), "chunks, there were", len(moar), "more")
	// Copy in reverse order to copy in reverse height order.
	// TODO: need assertions.
	for i := len(missing) - 1; i >= 0; i-- {
		r := missing[i]
		fmt.Println("writing ref", r.TargetRef().String(), "of height", r.Height())
		sink.batchStore().SchedulePut(source.batchStore().Get(r.TargetRef()), types.Hints{})
	}
	fmt.Println("flushing")
	sink.batchStore().Flush()

	/*
		excludeRefs := map[ref.Ref]bool{}

		if !exclude.TargetRef().IsEmpty() {
			mu := sync.Mutex{}
			excludeCallback := func(r types.Ref) bool {
				mu.Lock()
				excludeRefs[r.TargetRef()] = true
				mu.Unlock()
				return false
			}

			walk.SomeChunksP(exclude, source, excludeCallback, concurrency)
		}

		copyCallback := func(r types.Ref) bool {
			return excludeRefs[r.TargetRef()]
		}
		copyWorker(source, sink, sourceRef, copyCallback, concurrency)
	*/
}

func copyWorker(source Database, sink Database, sourceRef types.Ref, stopFn walk.SomeChunksCallback, concurrency int) {
	bs := sink.batchSink()
	walk.SomeChunksP(sourceRef, newTeeDataSource(source.batchStore(), bs), stopFn, concurrency)

	bs.Flush()
}

// teeDataSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeDataSource struct {
	source types.BatchStore
	sink   batchSink
}

func newTeeDataSource(source types.BatchStore, sink batchSink) *teeDataSource {
	return &teeDataSource{source, sink}
}

func (tds *teeDataSource) ReadValue(r ref.Ref) types.Value {
	c := tds.source.Get(r)
	if c.IsEmpty() {
		return nil
	}
	tds.sink.SchedulePut(c, types.Hints{})
	return types.DecodeChunk(c, tds)
}
