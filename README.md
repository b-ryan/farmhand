# Farmhand

[![CircleCI](https://circleci.com/gh/b-ryan/farmhand.svg?style=svg)](https://circleci.com/gh/b-ryan/farmhand)

Farmhand is a Clojure library for queuing jobs to be processed in the
background. It is backed by Redis to enable ease of use, flexibility, and great
performance.

In addition to the library, there is a
[web interface](https://github.com/b-ryan/farmhand-ui) available to easily see
which jobs are running, view and re-queue failed jobs, and more.

This project is largely inspired by
[Sidekiq](https://github.com/mperham/sidekiq) and
[RQ](https://github.com/nvie/rq).

**Warning** This library is beta software and the API is subject to change.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Documentation](#documentation)
- [Web Interface](#web-interface)
- [LICENSE](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Installation

Leiningen:

```clojure
[com.buckryan/farmhand "0.9.1"]
```

## Usage

Before starting, you need to have a Redis server running. Visit
[redis.io](https://redis.io/) to download. This example assumes Redis is running
on localhost.

Farmhand is designed for both ease of use and power. Below is an example showing
you the most common usage, but be sure to check out the
[Wiki](https://github.com/b-ryan/farmhand/wiki) to see what else it can do.

```clojure
;; STEP 1: Require the Farmhand namespace
(require '[farmhand.core :as farmhand])

;; STEP 2: Instantiate a Farmhand server with 4 workers.
(farmhand/start-server {:redis {:host "localhost"}
                        :num-workers 4})

;; STEP 3: Jobs are regular ol' Clojure functions:
(defn my-long-running-function
  [a b]
  (println "starting long-running function")
  (Thread/sleep 20000)
  (println "exiting long-running function")
  {:farmhand/result (* a b)})

;; STEP 4: Queue that job! It will be processed by the running Farmhand server.
(farmhand/enqueue {:fn-var #'my-long-running-function
                   :args [1 2]})

;; STEP 5: Stop the server. This will allow any running jobs to complete.
(farmhand/stop-server)
```

## Features

- Reliable insofar as Redis is. Dequeue operations are performed using a Lua
  [script](https://github.com/b-ryan/farmhand/blob/master/resources/farmhand/dequeue.lua)
  which Redis guarantees to be
  [atomic](https://redis.io/commands/eval#atomicity-of-scripts).
- Supports reading from multiple queues. See the
  [Queues](https://github.com/b-ryan/farmhand/wiki/Queues) documentation for
  details.
- Job processing is fully customizable through middleware support. See the
  [Middleware](https://github.com/b-ryan/farmhand/wiki/Middleware) docs.
- Jobs can be scheduled to run at a later time. The docs are
  [here](https://github.com/b-ryan/farmhand/wiki/Scheduling)
- Automatic job retrying. It's as simple as

  ```clojure
  (enqueue {:fn-var #'job-function :retry {:strategy :backoff}})
  ```

  This causes your job to retry 8 times over about 10 days. The retry mechanism
  is also fully customizable.
  [Docs](https://github.com/b-ryan/farmhand/wiki/Retrying-Jobs).

## Documentation

The bulk of the documentation is available in the
[Wiki](https://github.com/b-ryan/farmhand/wiki).

API docs are available [here](http://farmhand-clj.com/codox/index.html).

## Web Interface

The [Farmhand UI](https://github.com/b-ryan/farmhand-ui) project provides a web
interface for Farmhand. Hop over to that project to get started. Here's a
preview of what it looks like:

![Screenshot](https://github.com/b-ryan/farmhand-ui/raw/master/preview.png)

## LICENSE

Please see [LICENSE](https://github.com/b-ryan/farmhand/blob/master/LICENSE)
for details.
