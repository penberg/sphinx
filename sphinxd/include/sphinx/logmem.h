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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <vector>

/// \defgroup logmem-module Log-structured memory allocator.
///
/// Log-structured memory allocator manages main memory as a log to improve
/// memory utilization. The allocator manages memory in fixed-size segments,
/// which are arranged as vector of lists, sorted by amount of memory remaining
/// for allocation in the segment. Segments can hold objects of different sizes,
/// which eliminates internal fragmentation (object size being smaller than
/// allocation size) and also reduces external fragmentation (available memory
/// is in such small blocks that objects cannot be allocated from them) because
/// segments are expired in full.
///
/// The main entry point to the log-structured memory allocator is the \ref
/// Log::append() function, which attempts to append a key-value pair to the
/// log. The function allocates memory one segment at a time. That is, all
/// allocations are satisfied by the same segment until it runs out of memory.
/// Furthermore, the allocator first exhausts all memory it manages before
/// attempting to reclaim space by expiring segments.

namespace sphinx::logmem {

/// \addtogroup logmem-module
/// @{

/// Object key type.
using Key = std::string_view;

/// Object blob type.
using Blob = std::string_view;

/// An object in a segment of a log.
class Object final
{
  uint32_t _key_size;
  uint32_t _blob_size;
  uint32_t _expiration;

public:
  /// \brief Return the size of an object of \ref key and \ref blob.
  static size_t size_of(const Key& key, const Blob& blob);
  /// \brief Return the size of an object of \ref key_size and \ref blob_size.
  static size_t size_of(size_t key_size, size_t blob_size);
  /// \brief Construct a \ref Object instance.
  Object(const Key& key, const Blob& blob);
  /// \brief Expire object.
  void expire();
  /// \brief Return true if object is expired; otherwise return false.
  bool is_expired() const;
  /// \brief Returns the size of the object in memory.
  size_t size() const;
  /// \brief Return object key.
  Key key() const;
  /// \brief Return object blob.
  Blob blob() const;

private:
  const char* key_start() const;
  const char* blob_start() const;
};

/// A segment in a log.
class Segment
{
  char* _pos;
  char* _end;

public:
  /// \brief Construct a \ref Segment instance.
  Segment(size_t size);
  /// \brief Return true if segment has no objects; otherwise return false;
  bool is_empty() const;
  /// \brief Return true if segment is full of objects; otherwise return false;
  bool is_full() const;
  /// \brief Returns the number of bytes allocated for objects in this segment.
  size_t size() const;
  /// \brief Returns the number of bytes occupying the segment.
  size_t occupancy() const;
  /// \brief Returns the number of bytes available in the segment.
  size_t remaining() const;
  /// \brief Reset the segment into a clean segment.
  void reset();
  /// \brief Append an object represented by \ref key and \ref blob to the log.
  Object* append(const Key& key, const Blob& blob);
  /// \brief Return a pointer to the first object in the segment.
  Object* first_object();
  /// \brief Return a pointer to the next object immediatelly following \ref object.
  Object* next_object(Object* object);

private:
  char* start();
  const char* start() const;
};

struct LogConfig
{
  char* memory_ptr;
  size_t memory_size;
  size_t segment_size;
};

/// A log of objects.
class Log
{
  std::unordered_map<Key, Object*> _index;
  std::vector<Segment*> _segment_ring;
  size_t _segment_ring_head = 0;
  size_t _segment_ring_tail = 0;
  LogConfig _config;

public:
  /// \brief Construct a \ref Log instance.
  Log(const LogConfig& config);
  /// \brief Find for a blob for a given \ref key from the log.
  std::optional<Blob> find(const Key& key) const;
  /// \brief Append a key-blob pair to the log.
  bool append(const Key& key, const Blob& blob);
  /// \brief Remove the given \ref key from the log.
  bool remove(const Key& key);

private:
  bool try_to_append(const Key& key, const Blob& blob);
  bool try_to_append(Segment* segment, const Key& key, const Blob& blob);
  size_t expire(size_t reclaim_target);
  size_t expire(Segment* segment);
  size_t segment_index(size_t size);
};

/// @}
}
