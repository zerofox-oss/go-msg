# go-msg

[![GoDoc](https://godoc.org/github.com/zerofox-oss/go-msg?status.svg)](http://godoc.org/github.com/zerofox-oss/go-msg)
[![Build Status](https://travis-ci.org/zerofox-oss/go-msg.svg?branch=master)](https://travis-ci.org/zerofox-oss/go-msg)

**Pub/Sub Message Primitives for Go**

This library contains
the basic primitives
for developing [pub-sub][] systems.

*Messages* are published to *Topics*.
*Servers* subscribe to *Messages*.

These primitives specify
abstract behavior of pub-sub;
they do not specify implementation.
A *Message* could exist in
an in-memory array,
a file,
a key/value store like RabbitMQ,
or even something like
Amazon SNS/SQS
or Google Pub/Sub.
In order to tap into
that backend,
a concrete implementation
must be written for it.

Here's a list of backends
that are currently supported:

| Backend       | Link     |
| :------------- | :------------- |
| Channel-based  | [go-msg/mem](https://github.com/zerofox-oss/go-msg) |
| Amazon AWS (SNS,SQS) | [go-aws-msg](https://github.com/zerofox-oss/go-aws-msg) |

## How it works

### Backend

A backend simply represents
the infrastructure behind a pub-sub system.
This is where *Messages* live.

Examples could include a key/value store,
Google Pub/Sub, or Amazon SNS + SQS.

### Message

A *Message* represents a discrete unit of data.
It contains a body
and a list of attributes.
*Attributes* can be used to distinguish
unique properties of a *Message*,
including how to read the body.
More on that in [decorator patterns][].

### Publish

A *Message* is published to a *Topic*.
A *Topic* writes the body
and attributes of a *Message*
to a backend using a *MessageWriter*.
A *MessageWriter* may only
be used for one *Message*,
much like a [net/http ResponseWriter][http_responsewriter]

When the *MessageWriter* is closed,
the data that was written to it
will be published to that backend
and it will no longer be able to be used.

### Subscribe

A *Server* subscribes to *Messages*
from a backend.
It's important to note that
a *Server* must know how to convert
raw data to a *Message* -
this will be unique to each backend.
For example, the way you read
message attributes from a file
is very different from
how you read them from SQS.
A *Server* is always live,
so it will continue to
block indefinitely while
it is waiting for messages
until it is shut down.

When a *Message* is created,
the *Server* passes it to
a *Receiver* for processing.
This is similar to how
[net/http Handler][http_handler] works.
A *Receiver* may return an error
if it was unable to process the *Message*.
This will indicate to the *Server*
that the *Message* must be retried.
The specifics to this retry logic
will be specific to each backend.

## Benefits

This library was originally conceived
because we needed a way
to reduce copy-pasted code
across our pub-sub systems
and we wanted to try out other infrastructures.

These primitives allow us to
achieve both of those goals.
Want to try out Kafka instead of AWS?
No problem!
Just write a library
that utilizes these primitives
and the Kafka SDK.

What these primitives
or any implementation
of these primitives **DO NOT DO**
is mask or replace all of the functionality
of all infrastructures.
If you want to use
a particular feature of AWS
that does not fit
with these primitives,
that's OK.
It might make sense
to add that feature
to the primitives,
it might not.
We encourage you to open
an issue to discuss such additions.

Aside from the code re-use benefits,
there's a number of other features
which we believe are useful, including:

* Concrete implementations can be written once
  and distributed as libraries.

* [Decorator Patterns][].

* Built-in concurrency controls into *Server*.

* Context deadlines and cancellations.
  This allows for clean shutdowns to prevent data loss.

* Transaction-based *Receivers*.

[Decorator Patterns]: ./doc/decorator_patterns.md
[pub-sub]: https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern
[http_handler]: https://golang.org/pkg/net/http/#HandlerFunc.ServeHTTP
[http_responsewriter]: https://golang.org/pkg/net/http/#ResponseWriter
