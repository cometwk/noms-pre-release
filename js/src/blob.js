// @flow

import {Collection} from './collection.js';
import {IndexedSequence} from './indexed_sequence.js';
import {SequenceCursor} from './sequence.js';
import {invariant} from './assert.js';

export class NomsBlob extends Collection<IndexedSequence<number>> {
  // TODO: remove the |at| property for now.
  getReader(at: ?number): BlobReader {
    return new BlobReader(this.sequence.newCursorAt(at || 0));
  }
}

export class BlobReader {
  _cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>;
  _lock: boolean;

  constructor(cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>) {
    this._cursor = cursor;
    this._lock = false;
  }

  // TODO: make the return object type here an actual type?
  async read(): Promise<{done: boolean, value?: ArrayBuffer}> {
    invariant(!this._lock, 'cannot read without completing current read');
    this._lock = true;

    const cur = await this._cursor;
    if (!cur.valid) {
      // TODO: parens needed here?
      if (!(await cur.advance())) {
        return {done: true};
      }
      invariant(cur.valid);
    }

    // No more awaits after this, so we can't be interrupted.
    this._lock = false;

    const arr = new Uint8Array(cur.length);
    for (let i = 0; i < cur.length; i++) {
      invariant(cur.valid);
      const b = cur.getCurrent();
      invariant(b >= 0 && b < (1 << 8));
      arr[i] = b;
      // TODO: Make this better. I'd use advance() if it weren't async.
      cur.idx++;
    }

    invariant(!cur.valid);
    return {done: false, value: arr.buffer};
  }
}

export class BlobLeafSequence extends IndexedSequence<number> {
  getOffset(idx: number): number {
    return idx;
  }
}
