# Simple benchmark

Run the following to execute the `bench` command:

```
$ # From project root build in Release mode
$ rm -rf build
$ cmake -S . -B build -DCMAKE_BUILD_TYPE='Release'
...
$ cmake --build build
...
$ ./build/examples/bench/bench
Processed 10000000 rows in 1.81s (5517006 rows/sec)
```
