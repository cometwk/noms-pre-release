# Nomusic

Plays music!

## Get some music

Use the `fs` noms client to import an mp3 file:

```
fs ...
```

## Build

```
npm install
npm run build
```

## Run

```
noms-view serve . store=/tmp/nomusic:nomusic
```

Then, navigate to the URL printed by noms-view, e.g. http://127.0.0.1:12345?store=xyz.

## Develop

```
npm run start
noms-view serve .
```
