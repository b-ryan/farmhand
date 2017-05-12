# Change Log

## 0.9.0

This is a major new release! A huge portion of the codebase and the way
Farmhand works has been redone. Data is stored in Redis differently. Since no
one else is using this I'm just going to leave it at that.

## 0.8.0

FEATURES

- Customizable Redis prefix. Read about the motivation for this feature
  [here](https://github.com/b-ryan/farmhand/issues/3).

FIXES

- Exceptions logged when job is being retried. Fixes
  [this](https://github.com/b-ryan/farmhand/issues/5) issue.

BREAKING

- Completely refactored Jedis handling code. Some background info is in the
  commit where the change was made:
  [here](https://github.com/b-ryan/farmhand/commit/3abc96e6a078332df023ab8b1fa9d49c9b50af04)
- As a result of the above change, many of the functions (except in `core`)
  have had breaking changes. If this library were actually being used by anyone
  yet I might not have made such a drastic change :) But currently I can get
  away with it.
- `farmhand.dead-letters` namespace is gone. The two functions in it are now in
  `farmhand.queue`.

## 0.7.1

FEATURES

- `farmhand.queue/describe-queues` function to describe queues and their sizes
- `farmhand.queue/purge` function to clear out a queue

## 0.7.0

FEATURES

- Job scheduling.
- Automatic job retrying.

See the Wiki documentation for details.

BREAKING

- Argument order to `farmhand.core/enqueue` has changed. The `pool` is now the
  first argument. But you can continue to omit this argument to have the
  default pool be used. This was done in an effort to make the argument order
  more consistent across the codebase.
- `farmhand.handler` namespace has been redone. Most of the public functions
  have been renamed or removed. However the `default-handler` still works the
  same, so if you don't have a custom handler you won't be affected.

## 0.6.0

FEATURES

- You can now use a custom handler for processing jobs. The job processing
  functionality has been rewritten as a series of middleware functions.

## 0.5.0

DEPENDENCY CHANGES

- Farmhand now has Clojure core.async as a project dependency

FEATURES

- In addition to the worker threads, each farmhand server will now launch an
  additional thread which is responsible for cleaning data out of Redis. At
  this time Farmhand keeps all jobs and registry data for 60 days before it is
  cleaned out.

BREAKING

- Expiration of data now behaves differently. Previously, only successful jobs
  were given a TTL in Redis. Additionally all registry data was never removed.
  Now job and registry data will be expired 60 days from the time it is created
  (note that re-queueing does not affect the TTL).
- Some internal functions, Eg. in `utils` and `work` namespaces have been
  removed or changed.

## 0.4.0

BREAKING

- The interface of the `farmhand.config` namespace has been completely redone.

## 0.3.0

BREAKING

- `farmhand.registry/page` returns data in a slightly different format. See
  commit 6d1718f38808a85badd7da02fe62158d5c3dfc80 for details.

## 0.2.0

FEATURES

- `core` namespace now contains `server*` and `pool*` atoms that contain the
  most recently created Farmhand server and pool.
- `enqueue` and `stop-server` functions now have a second arity that defaults
  to using the values in the `pool*` and `server*` atoms.

## 0.1.0 - 2017-01-18

- First release
