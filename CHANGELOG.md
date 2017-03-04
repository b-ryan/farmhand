# Change Log

## 0.6.0 (Unreleased)

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

- **Breaking** The interface of the `farmhand.config` namespace has been
  completely redone.

## 0.3.0

- **Breaking** `farmhand.registry/page` returns data in a slightly different
  format. See commit 6d1718f38808a85badd7da02fe62158d5c3dfc80 for details.
- Threading is now implemented using
  `java.util.concurrent.Executors/newFixedThreadPool` instead of using Clojure
  `future`s.

## 0.2.0

- `core` namespace now contains `server*` and `pool*` atoms that contain the
  most recently created Farmhand server and pool.
- `enqueue` and `stop-server` functions now have a second arity that defaults
  to using the values in the `pool*` and `server*` atoms.

## 0.1.0 - 2017-01-18

- First release
