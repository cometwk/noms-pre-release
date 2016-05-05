// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {default as diff, RefQueue} from './chunk-diff.js';
import ValueStore from './value-store.js';
import {getRefOfValue} from './get-ref.js';
import {newList} from './list.js';
import {default as RefValue, constructRefValue} from './ref-value.js';
import {makeRefType, numberType} from './type.js';
import {getChunksOfValue} from './value.js';

const testSize = 2000;

function numsFromTo(from: number, to: number): [number] {
  return [...Array(to - from)].map((_, i) => i + from);
}

suite('ChunkDiff', () => {

  test('RefQueue', () => {
    let unique = 0;
    const refWithHeight = (height: number): RefValue => {
      return constructRefValue(makeRefType(numberType), getRefOfValue(unique++), height);
    }

    const q = new RefQueue();
    const [r1a, r1b, r2a, r2b, r3, r4] = [1, 1, 2, 2, 3, 4].map(refWithHeight);

    assert.isTrue(q.isEmpty());

    q.offer(r4);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(4, q.getHeight());

    assert.deepEqual([r4], q.poll());
    assert.isTrue(q.isEmpty());

    q.offer(r2a);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(2, q.getHeight());

    q.offer(r3);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(3, q.getHeight());

    q.offer(r1a);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(3, q.getHeight());

    q.offer(r2b);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(3, q.getHeight());

    assert.deepEqual([r3], q.poll());
    assert.isFalse(q.isEmpty());
    assert.strictEqual(2, q.getHeight());

    q.offer(r1b);
    assert.isFalse(q.isEmpty());
    assert.strictEqual(2, q.getHeight());

    assert.deepEqual([r2a, r2b], q.poll());
    assert.isFalse(q.isEmpty());
    assert.strictEqual(1, q.getHeight());

    assert.deepEqual([r1a, r1b], q.poll());
    assert.isTrue(q.isEmpty());
  });

  async function assertDiffIsConsistent(r1: RefValue, r2: RefValue, vs: ValueStore): Promise<void> {
    const getChunkGraph = async (r: RefValue) => {
      const val = await r.targetValue(vs);
      const descend = await Promise.all(getChunksOfValue(val).map(getChunkGraph));
      const res = [r];
      descend.forEach(rs => res.push(...rs));
      return res;
    };

    // Test using a Set<string> to make lookup easier.
    const toRefStringSet = (rs: [RefValue]) => new Set(rs.map(r => r.targetRef.toString()));

    const graph1 = toRefStringSet(await getChunkGraph(r1));
    const graph2 = toRefStringSet(await getChunkGraph(r2));
    const [only1, only2] = (await diff(vs, r1, r2)).map(toRefStringSet);

    for (const r of only1) {
      assert.isTrue(graph1.has(r), `graph1 is missing ${r}`);
      assert.isFalse(graph2.has(r), `graph2 should not contain ${r}`);
    }

    for (const r of only2) {
      assert.isFalse(graph1.has(r), `graph1 should not contain ${r}`);
      assert.isTrue(graph2.has(r), `graph2 is missing ${r}`);
    }

    const union = (s1: Set, s2: Set) => {
      // Mocha doesn't know how to compare Sets, so convert to sorted list.
      // $FlowIssue: Flow doesn't know that Set is compatible with spread.
      const res = [...s1, ...s2];
      res.sort();
      return res;
    };

    assert.deepEqual(union(graph1, only2), union(graph2, only1),
                     `union of graphs with their diffs are not equal`);
  }

  async function testDiffSplice(idx: number, del: number, ...ins: Array<number>): Promise<void> {
    const vs = new ValueStore(new makeTestingBatchStore());
    const list = await newList(numsFromTo(0, testSize));
    const listR = vs.writeValue(list);
    return list.splice(idx, del, ...ins)
      .then(spl => assertDiffIsConsistent(listR, vs.writeValue(spl), vs));
  }

  test('diff - empty diff', async () => {
    const vs = new ValueStore(new makeTestingBatchStore());
    const list = await newList(numsFromTo(0, testSize));
    const listR = vs.writeValue(list);
    await assertDiffIsConsistent(listR, vs.writeValue(await newList([])), vs);
  });

  test('diff - no-op', () => testDiffSplice(0, 0));

  test('diff - small number of items', () => {
    const next10 = numsFromTo(testSize, testSize + 10);
    return Promise.all([
      // Start
      testDiffSplice(0, 0, ...next10),
      testDiffSplice(0, 10),
      testDiffSplice(0, 10, ...next10),
      // Middle
      testDiffSplice(testSize / 2, 0, ...next10),
      testDiffSplice(testSize / 2, 10),
      testDiffSplice(testSize / 2, 10, ...next10),
      // End
      testDiffSplice(testSize, 0, ...next10),
      testDiffSplice(testSize - 10, 10),
      testDiffSplice(testSize - 10, 10, ...next10),
    ]);
  });

  test('diff - half the number of items', async () => {
    const half = numsFromTo(testSize, testSize + testSize / 2);
    return Promise.all([
      // Start
      testDiffSplice(0, 0, ...half),
      testDiffSplice(0, testSize / 2),
      testDiffSplice(0, testSize / 2, ...half),
      // Middle
      testDiffSplice(testSize / 4, 0, ...half),
      testDiffSplice(testSize / 4, testSize / 2),
      testDiffSplice(testSize / 4, testSize / 2, ...half),
      // End
      testDiffSplice(testSize, 0, ...half),
      testDiffSplice(testSize / 2, testSize / 2),
      testDiffSplice(testSize / 2, testSize / 2, ...half),
    ]);
  });
});
