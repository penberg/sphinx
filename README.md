Sphinx
======

What is Sphinx?
---------------

Sphinx is a [Memcached](http://memcached.org/)-compatible memory cache system that uses thread-per-core architecture, non-blocking networking APIs, and a log-structured memory allocator similar to (Rumble _et al._, 2014) to provide high throughput and consistent low latency on multicore systems.

Requirements
------------

To build Sphinx, you need:

* C++17 compatible compiler, such as [GCC 7](https://gcc.gnu.org/) or [Clang 5](https://clang.llvm.org/)
* [CMake](https://cmake.org/)
* [Ragel](http://www.colm.net/open-source/ragel/)
* [Google Test](https://github.com/google/googletest)
* [Google Benchmark library](https://github.com/google/benchmark)

Build
-----

To build Sphinx, run the following commands:

    mkdir build
    cd build
    cmake ..
    make

Usage
-----

To start Sphinx, run the following command in the build directory:

    sphinxd/sphinxd

References
----------

Stephen M. Rumble, Ankita Kejriwal, and John Ousterhout. 2014. Log-structured memory for DRAM-based storage. In _Proceedings of the 12th USENIX conference on File and Storage Technologies_ (FAST'14). USENIX Association, Berkeley, CA, USA, 1-16.
