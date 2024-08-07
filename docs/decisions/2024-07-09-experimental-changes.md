---
Date: 2024/07/09
Authors: @Xopherus, @shezadkhan137
Status: Accepted
---

# Experimental Libraries 

## Context

Go-msg is an open source library, although primarily used internally (at ZeroFox).
There are new features and changes we wish to implement, 
although it is sometimes difficult to merge because we have
a higher standard for excellence (given the code is open source).

It would be useful to have a mechanism to indicate that changes we introduce can be experimental (not production ready),
and should be used at your own risk. 
This would allow us to innovate with additional backends, decorators, and primitives
without changing the core of the library
until we are confident that such changes should be accepted.

## Decision

We will create an `go-msg/x/` directory which will house experimental features.
We chose this model after [Go's experimental libraries](https://pkg.go.dev/golang.org/x/exp#section-readme).

As a caveat, packages here are **experimental and unreliable**. They may be promoted to the main package,
modified entirely, or removed. 

## Consequences

Every experimental package introduced should be accompanied with a Decision Record, 
indicating what we're adding and why.