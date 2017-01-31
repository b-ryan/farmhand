# Change Log

## Unreleased

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
