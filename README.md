# Golang Client for Ministream

[![license](https://img.shields.io/github/license/nbigot/ministream-client-go)](https://github.com/nbigot/ministream-client-go/blob/main/LICENSE)


## Overview

Package `ministream-client-go` is a *Ministream* client in Go.

It connects to a *Ministream* server and provides a producer and a consumer.


## Installation

Get the package source:

    $ go get github.com/nbigot/ministream-client-go


## Profiling

Run program and create benchmark profile files:

```sh
$ go test -cpuprofile cpu.prof -memprofile mem.prof -bench .
```


Analyse profile files:

```sh
$ go tool pprof cpu.prof
(pprof) top 20 -cum
(pprof) web
```


Generate and analyse trace:

```sh
$ go test -trace=trace.out
$ go tool trace trace.out
$ go tool trace -pprof=net trace.out > network.out
```


## Contribution guidelines

If you want to contribute to *ministream-client-go*, be sure to review the [code of conduct](CODE_OF_CONDUCT.md).


## License

This software is licensed under the [MIT](./LICENSE).
