/*
Copyright 2018 The Sphinxd Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#pragma once

#include <array>
#include <atomic>

/// \defgroup spsc-queue-module A bounded, single-producer/single-consumer (SPSC) wait-free and
/// lock-free queue.
///
/// A SPSC queue is a ring buffer with two indexes to the ring: head and tail. A producer writes new
/// entries in the queue after the current tail and a consumer reads entries from the head.
///
/// While the algorithm was implemented independently, the following implementations were used as
/// reference:
///
///  https://www.scylladb.com/2018/02/15/memory-barriers-seastar-linux/
///  https://github.com/rigtorp/SPSCQueue
///  https://github.com/fsaintjacques/disruptor--
///
/// Furthermore, the same algorithm is described in the following paper (including proof of
/// correctness):
///
///   Lê, N.M., Guatto, A., Cohen, A. and Pop, A., 2013, October. Correct and
///   efficient bounded FIFO queues. In Computer Architecture and High Performance
///   Computing (SBAC-PAD), 2013 25th International Symposium on (pp. 144-151).
///   IEEE.

namespace sphinx::spsc {

constexpr int cache_line_size = 128;

/// \addtogroup spsc-queue-module
/// @{

template<typename T, size_t N>
class Queue
{
  alignas(cache_line_size) std::atomic<size_t> _head = 0;
  alignas(cache_line_size) std::atomic<size_t> _tail = 0;
  std::array<T, N> _data;

public:
  bool empty() noexcept
  {
    return _head.load(std::memory_order_acquire) == _tail.load(std::memory_order_acquire);
  }
  template<typename... Args>
  bool try_to_emplace(Args&&... args) noexcept
  {
    auto tail = _tail.load(std::memory_order_relaxed);
    auto next_tail = tail + 1;
    if (next_tail == N) {
      next_tail = 0;
    }
    if (next_tail == _head.load(std::memory_order_acquire)) {
      return false;
    }
    _data[tail] = T{std::forward<Args>(args)...};
    // The release fence here ensures that message is constructed before we update the tail. This
    // prevents the consumer from reading stale messages.
    _tail.store(next_tail, std::memory_order_release);
    return true;
  }
  T* front() noexcept
  {
    auto head = _head.load(std::memory_order_relaxed);
    if (_tail.load(std::memory_order_acquire) == head) {
      return nullptr;
    }
    return &_data[head];
  }
  void pop() noexcept
  {
    auto head = _head.load(std::memory_order_relaxed);
    auto next_head = head + 1;
    if (next_head == N) {
      next_head = 0;
    }
    _data[head].~T();
    // The release fence here ensures that message is destructed before we update the head index.
    // This prevents the producer from reusing memory for a message, which will be destructed later
    // thus corrupting the new message.
    _head.store(next_head, std::memory_order_release);
  }
};

/// @}
}
