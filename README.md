
<img src='doc/nommy_cropped_smaller.png' align='left' width='350' title='Nommy, the snacky otter'>
<p align='right'>
[![Build Status](http://jenkins.noms.io/job/NomsServer/badge/icon)](http://jenkins.noms.io/job/NomsServer)

<br clear='all'/>

*Noms* is a decentralized database based on ideas from Git.

This repository contains two reference implementations of the database—one in Go, and one in JavaScript. It also includes a number of tools and sample applications.

## About Noms

Noms is different from other databases. It is:

* *Content-addressed*. If you have some data you want to put into Noms, you don't have to worry about whether it already exists. Duplicate data is automatically ignored. There is no update, only insert.

* *Append-only*. When you commit data to Noms, you aren't overwriting anything. Instead you're adding to a historical record. By default, data is never removed from Noms. You can see the entire history of the database, diff any two commits, or rewind to any previous point in time.

* *Strongly-typed*. Noms doesn't have schemas that you design up front. Instead, each version of a Noms database has a *type*, which is generated automatically as you add data. You can write code against the type of a Noms database, confident that you've handled all the cases you need to.

* *Decentralized*. If I give you a copy of my database, you and I can modify our copies disconnected from each other, and come back together and merge our changes efficiently and correctly days, weeks, or years later.

## Setup

Noms is supported on Mac OS X and Linux. Windows usually works, but isn't officially supported.

1. Install [Go 1.6+](https://golang.org/dl/)
2. Ensure your [$GOPATH](https://github.com/golang/go/wiki/GOPATH) is configured
3. Type type type:
```
git clone https://github.com/attic-labs/noms $GOPATH/src/github.com/attic-labs/noms
go install github.com/attic-labs/noms/cmd/noms

noms log http://demo.noms.io/cli-tour::sf-film-locations
```
[Command-Line Tour](doc/cli-tour.md)&nbsp; | &nbsp;[Go SDK Tour](doc/go-tour.md)&nbsp; | &nbsp;[JavaScript SDK Tour](doc/js-tour.md)&nbsp; | &nbsp;[Intro to Noms](doc/intro.md)&nbsp; | &nbsp;[FAQ](doc/faq.md)

<br/>
## What Noms is Good For

#### Data Version Control

Noms gives you the entire Git workflow, but for large-scale structured (or unstructured) data. Fork, merge, track history, efficiently synchronize changes, etc.

[<img src="doc/data-version-control.png" width="320" height="180">](https://www.youtube.com/watch?v=Zeg9CY3BMes)<br/>
*[`noms diff` and `noms log` on large datasets](https://www.youtube.com/watch?v=Zeg9CY3BMes)*


#### An Application Database with History

A database where every change is automatically and efficiently preserved. Instantly revert to, fork, or work from any historical commit.

[<img src="doc/versioned-database.png" width="320" height="180">](https://www.youtube.com/watch?v=JDO3z0vHEso)<br/>
*[Versioning, Diffing, and Syncing with Noms](https://www.youtube.com/watch?v=JDO3z0vHEso)*


#### An Archival Database

Trivially import snapshots from any format or API. Data is automatically versioned and deduplicated. Track the history of each datasource. Search across data sources.

*TODO: Sample and video*


<br>
## Status

We are fairly confident in the core data format, and plan to support Noms database version `7` and forward. If you create a database with Noms today, future versions will have migration tools to pull your databases forward.

However, many important features are not yet implemented including a query system, concurrency, auto-merging, and GC.


<br>
## Talk

- [![Slack](http://slack.noms.io/badge.svg)](http://slack.noms.io)
- [Mailing List](https://groups.google.com/forum/#!forum/nomsdb)
- [Twitter](https://twitter.com/nomsdb)
