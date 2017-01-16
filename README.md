# Farmhand

Farmhand is a library for Clojure for queueing jobs to be processed in the
background. It is backed by Redis to enable ease of use, flexibility, and great
performance.

In addition to the library, there is a Web UI available to easily get insights
into your workers, view and re-queue failed jobs, and more.

This project is largely inspired by
[Sidekiq](https://github.com/mperham/sidekiq) and
[RQ](https://github.com/nvie/rq).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Installation](#installation)
- [Usage](#usage)
  - [Queuing Jobs](#queuing-jobs)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Installation

Leiningen:

```
[com.buckryan/farmhand "0.1.0-SNAPSHOT"]
```

## Usage

### Queuing Jobs

You need to first have a Redis server running. Visit
[redis.io](https://redis.io/) to download.

Defining jobs is dead simple. Take a look at this:

```clojure
(ns my.namespace
  ;; STEP 1: Require the Farmhand namespace
  (:require [com.buckryan.farmhand.core :refer [enqueue]))

;; STEP 2: Jobs are just regular ol' Clojure functions, they just need to be public:
(defn my-long-running-function
  [a b]
  (Thread/sleep 20000)
  (* a b)

;; STEP 3: Queue that job!
(enqueue {:queue "myjobs" ;; optional; defaults to "default"
          :fn-var #'my-long-running-function
          :args [1 2]})
```

That's it! Next you'll want to get a server running to process the job. Well
that's pretty simple too! The server can be embedded into an existing
application or run standalone. Here's how you embed it:

```clojure
(ns my.application
  ;; STEP 1: Require the Farmhand namespace
  (:require [com.buckryan.farmhand.core :refer [start-server stop-server])
  (:gen-class)


(defn -main
  [& args]
  ;; STEP 2: Start the server:
  (let [server (start-server)]
    ;; STEP 3: Run your application until you're ready to shut down
    ;; ...
    ;; STEP 4: Shut down the server. This will allow any running jobs to
    ;; complete:
    (stop-server server)))
```
