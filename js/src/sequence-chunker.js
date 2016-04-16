// @flow

import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import type {SequenceCursor} from './sequence.js';

export type BoundaryChecker<T> = {
  write: (item: T) => bool;
  windowSize: number;
}

export type NewBoundaryCheckerFn<T> = () => BoundaryChecker<T>;

export type makeChunkFn = (items: Array<any>, numLeaves: number) => [any, any];

export async function chunkSequence<S, T>(
  cursor: ?SequenceCursor,
  insert: Array<S>,
  remove: number,
  makeChunk: makeChunkFn,
  parentMakeChunk: makeChunkFn,
  boundaryChecker: BoundaryChecker<S>,
  newBoundaryChecker: NewBoundaryCheckerFn<T>): Promise<any> {

  const chunker = new SequenceChunker(cursor, makeChunk, parentMakeChunk, boundaryChecker,
                                      newBoundaryChecker);
  if (cursor) {
    await chunker.resume();
  }

  if (remove > 0) {
    invariant(cursor);
    for (let i = 0; i < remove; i++) {
      await chunker.skip(1);
    }
  }

  for (let i = 0; i < insert.length; i++) {
    chunker.append(insert[i], 1);
  }

  return chunker.done();
}

export class SequenceChunker<S, T, U:Sequence, V:Sequence> {
  _cursor: ?SequenceCursor<S, U>;
  _isOnChunkBoundary: boolean;
  _parent: ?SequenceChunker<T, T, V, V>;
  _current: Array<S>;
  _derivedSize: number;
  _makeChunk: makeChunkFn;
  _parentMakeChunk: makeChunkFn;
  _boundaryChecker: BoundaryChecker<S>;
  _newBoundaryChecker: NewBoundaryCheckerFn<T>;
  _used: boolean;

  constructor(cursor: ?SequenceCursor, makeChunk: makeChunkFn,
              parentMakeChunk: makeChunkFn,
              boundaryChecker: BoundaryChecker<S>,
              newBoundaryChecker: NewBoundaryCheckerFn<T>) {
    this._cursor = cursor;
    this._isOnChunkBoundary = false;
    this._parent = null;
    this._current = [];
    this._derivedSize = 0;
    this._makeChunk = makeChunk;
    this._parentMakeChunk = parentMakeChunk;
    this._boundaryChecker = boundaryChecker;
    this._newBoundaryChecker = newBoundaryChecker;
    this._used = false;
  }

  async resume(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (cursor.parent) {
      this.createParent();
      await notNull(this._parent).resume();
    }

    // TODO: Only call maxNPrevItems once.
    const prev = await cursor.maxNPrevItems(this._boundaryChecker.windowSize - 1);
    for (let i = 0; i < prev.length; i++) {
      this._boundaryChecker.write(prev[i]);
    }

    // TODO: This doesn't need to await because it's within the current chunk,
    // which has already been paged in.
    this._current = await cursor.maxNPrevItems(cursor.indexInChunk);
    this._used = this._current.length > 0;

    // The derived size starts at the full size of the chunk, and in finalizeCursor() isn't
    // increased. Additional calls to append/skip will affect it.  TODO: I think this is wrong, it
    // should be using any sort of "derived size" concept. It should be building up the size and
    // then when chunks are created set that size. Ugh this is complicated.
    this._derivedSize = cursor.sequence.numLeaves;
  }

  append(item: S, numLeaves: number) {
    if (this._isOnChunkBoundary) {
      this.createParent();
      this.handleChunkBoundary();
      this._isOnChunkBoundary = false;
    }
    this._current.push(item);
    this._derivedSize += numLeaves;
    this._used = true;
    if (this._boundaryChecker.write(item)) {
      this.handleChunkBoundary();
    }
  }

  async skip(numLeaves: number): Promise<void> {
    const cursor = notNull(this._cursor);
    const numLeaves = cursor.sequence.numLeaves;
    if (await cursor.advance()) {
      this._derivedSize -= numLeaves;
      if (cursor.indexInChunk === 0) {
        return this.skipParentIfExists();
      }
    }
  }

  async skipParentIfExists(): Promise<void> {
    if (this._parent && this._parent._cursor) {
      await this._parent.skip();
    }
  }

  createParent() {
    invariant(!this._parent);
    this._parent = new SequenceChunker(
        this._cursor && this._cursor.parent ? this._cursor.parent.clone() : null,
        this._parentMakeChunk,
        this._parentMakeChunk,
        this._newBoundaryChecker(),
        this._newBoundaryChecker);
  }

  handleChunkBoundary() {
    invariant(this._current.length > 0);
    if (!this._parent) {
      invariant(!this._isOnChunkBoundary);
      this._isOnChunkBoundary = true;
    } else {
      invariant(this._current.length > 0);
      const chunk = this._makeChunk(this._current, this._derivedSize)[0];
      // No, this shouldn't be the derived size.
      notNull(this._parent).append(chunk, this._derivedSize);
      this._current = [];
      this._derivedSize = 0;
    }
  }

  async done(): Promise<any> {
    if (this._cursor) {
      await this.finalizeCursor();
    }

    if (this.isRoot()) {
      return this._makeChunk(this._current)[1];
    }

    if (this._current.length > 0) {
      this.handleChunkBoundary();
    }

    invariant(this._parent);
    return this._parent.done();
  }

  isRoot(): boolean {
    for (let ancestor = this._parent; ancestor; ancestor = ancestor._parent) {
      if (ancestor._used) {
        return false;
      }
    }

    return true;
  }

  async finalizeCursor(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (!cursor.valid) {
      await this.skipParentIfExists();
      return;
    }

    const fzr = cursor.clone();
    let i = 0;
    for (; i < this._boundaryChecker.windowSize || fzr.indexInChunk > 0; i++) {
      if (i === 0 || fzr.indexInChunk === 0) {
        await this.skipParentIfExists();
      }
      // numLeaves is 0 because _derivedSize 
      this.append(fzr.getCurrent(), 0);
      if (!await fzr.advance()) {
        break;
      }
    }
  }
}
