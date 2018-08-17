Sphinx
======

What is Sphinx?
---------------

Sphinx is a fast in-memory key-value store that is compatible with the [Memcached](http://memcached.org/) wire protocol.

Sphinx partitions data between logical cores, similar to MICA (Lim _et al._, 2014), so that a specific core manages each key. Sphinx also partitions connection sockets between cores.  If a remote core manages a request key, Sphinx uses message passing to execute the request on that core.  To manage key-value pairs, Sphinx uses an in-memory, log-structured memory allocator, similar to RAMCloud (Rumble _et al._, 2014).

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

Hyeontaek Lim, Dongsu Han, David G. Andersen, and Michael Kaminsky. 2014. MICA: a holistic approach to fast in-memory key-value storage. In _Proceedings of the 11th USENIX Conference on Networked Systems Design and Implementation (NSDI'14)_. USENIX Association, Berkeley, CA, USA, 429-444.

Stephen M. Rumble, Ankita Kejriwal, and John Ousterhout. 2014. Log-structured memory for DRAM-based storage. In _Proceedings of the 12th USENIX conference on File and Storage Technologies (FAST'14)_. USENIX Association, Berkeley, CA, USA, 1-16.
