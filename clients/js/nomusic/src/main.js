// @flow

import {
  Dataset,
  DataStoreSpec,
  NomsBlob,
  walk,
} from '@attic/noms';
import React from 'react';
import ReactDOM from 'react-dom';
import Music from './music.js';

window.onload = render;
window.onresize = render;

const searchParams = {};
{
  const searchParamsIdx = location.href.indexOf('?');
  if (searchParamsIdx > -1) {
    decodeURIComponent(location.href.slice(searchParamsIdx + 1))
    .split('&')
    .forEach(pair => {
      const [k, v] = pair.split('=');
      searchParams[k] = v;
    });
  }
}

async function main(): Promise<React.Element> {
  if (!searchParams.store) {
    return <div>Must specify a <ttt>store</ttt></div>;
  }
  if (!searchParams.set) {
    return <div>Must specify a <ttt>set</ttt></div>;
  }

  const spec = DataStoreSpec.parse(location.origin + searchParams.store);
  if (!spec) {
    return <div>Failed to parse spec <ttt>{searchParams.store}</ttt></div>;
  }

  const store = spec.store();
  const set = new Dataset(store, searchParams.set);

  const head = await set.head();
  if (!head) {
    return <div>No music.</div>;
  }

  const music = [];

  await walk(head, store, v => {
    let con = true;
    if (v instanceof NomsBlob) {
      music.push(<Music blob={v} key={v.ref}/>);
      con = false;
    }
    return Promise.resolve(con);
  });

  return <div>{music}</div>;
}

function render() {
  main().then(m => ReactDOM.render(m, document.querySelector('#main')));
}
