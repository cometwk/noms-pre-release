// @flow

import {invariant, notNull} from './assert.js';
import {compare} from './compare.js';
import RefValue from './ref-value.js';
import {ValueReader} from './value-store.js';

// RefQueue is a priority queue-like datastructure of RefValue objects, optimised for the RefValue
// diff algorithm. Compared to a general purpose (e.g. heap) priority queue:
//  - offer() has equivalent semantics. O(number of heights), but see NumPQ for why this is fine.
//  - getHeight() instead of peek, because diff only cares about peeking at the height. O(1).
//  - poll() polls all RefValue objects with the largest height, instead of the single highest
//    priority RefValue, because diff works by considering each layer at a time. This means that
//    poll can trivially be O(1).
export class RefQueue {
  _nums: ?NumPQ;
  _refs: Map<number, [RefValue]>;

  constructor() {
    this._refs = new Map();
  }

  offer(...rs: Array<RefValue>) {
    rs.forEach(r => this._offer(r));
  }

  _offer(r: RefValue) {
    const height = r.height;
    const refsAtHeight = this._refs.get(height);

    if (refsAtHeight) {
      refsAtHeight.push(r);
    } else {
      if (this._nums) {
        this._nums.offer(height);
      } else {
        this._nums = new NumPQ(height);
      }

      this._refs.set(height, [r]);
    }
  }

  getHeight(): number {
    const head = this._nums;
    invariant(head, 'cannot get height of empty queue');
    return head.val;
  }

  poll(): [RefValue] {
    const head = this._nums;
    invariant(head, 'cannot poll empty queue');
    this._nums = head.tail;
    const res = notNull(this._refs.get(head.val));
    this._refs.delete(head.val);
    return res;
  }

  isEmpty(): boolean {
    return !this._nums;
  }
}

// NumPQ is a priority queue of RefValue heights implemented as a linked list. This is a reasonable
// implementation (versus a heap, for example) because heights will likely be added close to the
// head, given the way the graph is descended.
class NumPQ {
  val: number;
  tail: ?NumPQ;

  constructor(val: number, tail: ?NumPQ = null) {
    this.val = val;
    this.tail = tail;
  }

  offer(n: number) {
    invariant(n !== this.val, `${n} is already in the queue`);
    if (n > this.val) {
      this.tail = new NumPQ(this.val, this.tail);
      this.val = n;
    } else if (!this.tail) {
      this.tail = new NumPQ(n);
    } else if (n < this.val) {
      this.tail.offer(n);
    }
  }
}

// Computes the number of RefValue objects only reachable from rootA ("onlyA") and only reachable
// from rootB ("onlyB"), in O(onlyA.length + onlyB.length). Returns the tuple [onlyA, onlyB].
export default async function diff(
    vr: ValueReader, rootA: RefValue, rootB: RefValue): Promise<[[RefValue], [RefValue]]> {

  // For each ref in |newRefs|, add to |dest|, then offer its reachable chunks to |reachable|.
  const sync = (newRefs: [RefValue], reachable: RefQueue, dest: [RefValue]): Promise<void> => {
    dest.push(...newRefs);
    return Promise.all(newRefs.map(r => r.targetValue(vr))).then(vs => {
      vs.forEach(v => reachable.offer(...v.chunks));
    });
  };

  const reachableFromA = new RefQueue(), reachableFromB = new RefQueue();
  const onlyInA: [RefValue] = [], onlyInB: [RefValue] = [];

  reachableFromA.offer(rootA);
  reachableFromB.offer(rootB);

  while (!reachableFromA.isEmpty() && !reachableFromB.isEmpty()) {
    const heightA = reachableFromA.getHeight();
    const heightB = reachableFromB.getHeight();
    if (heightA > heightB) {
      const newRefs = pollTo(reachableFromA, heightB);
      await sync(newRefs, reachableFromA, onlyInA);
    } else if (heightB > heightA) {
      const newRefs = pollTo(reachableFromB, heightA);
      await sync(newRefs, reachableFromB, onlyInB);
    } else {
      const [newRefsA, newRefsB] = diffRefValueArrays(pollTo(reachableFromA, heightA - 1),
                                                      pollTo(reachableFromB, heightB - 1));
      await Promise.all([sync(newRefsA, reachableFromA, onlyInA),
                         sync(newRefsB, reachableFromB, onlyInB)]);
    }
  }

  onlyInA.push(...pollTo(reachableFromA, 0));
  onlyInB.push(...pollTo(reachableFromB, 0));

  return [onlyInA, onlyInB];
}

// Returns all RefValue objects in |reachable| that are higher than |floor| (exclusive).
function pollTo(reachable: RefQueue, floor: number): [RefValue] {
  const res = [];
  while (!reachable.isEmpty() && reachable.getHeight() > floor) {
    res.push(...reachable.poll());
  }
  return res;
}

// Sorts |sliceA| and |arrB|, then returns a tuple of [refs only in sliceA, refs only in arrB].
function diffRefValueArrays(sliceA: [RefValue], arrB: [RefValue]): [[RefValue], [RefValue]] {
  const onlyInA: [RefValue] = [], onlyInB: [RefValue] = [];
  let idxA = 0, idxB = 0;

  sliceA.sort(compare);
  arrB.sort(compare);

  while (idxA < sliceA.length && idxB < arrB.length) {
    const refA = sliceA[idxA], refB = arrB[idxB];
    switch (compare(refA, refB)) {
      case -1:
        onlyInA.push(refA);
        idxA++;
        break;
      case 0:
        idxA++;
        idxB++;
        break;
      case 1:
        onlyInB.push(refB);
        idxB++;
        break;
    }
  }

  onlyInA.push(...sliceA.slice(idxA));
  onlyInB.push(...arrB.slice(idxB));

  return [onlyInA, onlyInB];
}
