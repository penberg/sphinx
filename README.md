Sphinx
======

What is Sphinx?
---------------

Sphinx is a [Memcached](http://memcached.org/)-compatible memory cache system that uses thread-per-core architecture, non-blocking networking APIs, and a log-structured memory allocator similar to (Rumble _et al._, 2014) to provide high throughput and consistent low latency on multicore systems.

Requirements
------------

To build Sphinx, you need:

* CMake
* GCC or Clang version that supports C++17
* Ragel
* GoogleTest
* Google Benchmark library

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
