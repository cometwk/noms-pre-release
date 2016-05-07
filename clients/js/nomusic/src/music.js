// @flow

import {
  BlobReader,
  Dataset,
  DataStoreSpec,
  NomsBlob,
  walk,
} from '@attic/noms';
import React from 'react';
import ReactDOM from 'react-dom';

type Props = {
  blob: NomsBlob,
};

type State = {
  // $FlowIssue: Doesn't know about the Audio element constructor.
  audio: ?Audio,
};

export default class Music extends React.Component<void, Props, State> {
  state: State;

  constructor(props: Props) {
    super(props);
    this.state = {audio: null};
  }

  render(): React.Element {
    const {audio} = this.state;
    const icon = audio ? '\u25fc': '\u25b6'
    return <div><button onClick={() => this._toggle()}>{icon}</button></div>;
  }

  async _toggle() {
    let {audio} = this.state;
    if (audio) {
      audio.pause();
      this.setState({audio: null});
      return;
    }

    const {blob} = this.props;
    const arr = new Uint8Array(blob.length);
    const reader = await blob.getReader(); // this isn't async! but flow thinks it is

    for (let offset = 0, r = await reader.read(); !r.done; r = await reader.read()) {
      arr.set(r.value, offset);
      offset += r.value.length;
    }

    const blobURL = URL.createObjectURL(new Blob([arr]));
    audio = new window.Audio(blobURL);
    audio.play();
    this.setState({audio});
  }
}
