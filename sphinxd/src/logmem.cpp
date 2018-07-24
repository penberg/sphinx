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

#include <sphinx/logmem.h>

#include <cstring>
#include <iostream>

namespace sphinx::logmem {

Object::Object(const Key& key, const Blob& blob)
  : _key_size{uint32_t(key.size())}
  , _blob_size{uint32_t(blob.size())}
  , _expiration{0}
{
  std::memcpy(const_cast<char*>(key_start()), key.data(), _key_size);
  std::memcpy(const_cast<char*>(blob_start()), blob.data(), _blob_size);
}

size_t
Object::size_of(const Key& key, const Blob& blob)
{
  return Object::size_of(key.size(), blob.size());
}

size_t
Object::size_of(size_t key_size, size_t blob_size)
{
  return sizeof(Object) + key_size + blob_size;
}

size_t
Object::size() const
{
  return Object::size_of(_key_size, _blob_size);
}

void
Object::expire()
{
  _expiration = 1;
}

bool
Object::is_expired() const
{
  return _expiration;
}

Key
Object::key() const
{
  return Key{key_start(), _key_size};
}

Blob
Object::blob() const
{
  return Blob{blob_start(), _blob_size};
}

const char*
Object::key_start() const
{
  const char* obj_start = reinterpret_cast<const char*>(this);
  return obj_start + sizeof(Object);
}

const char*
Object::blob_start() const
{
  return key_start() + _key_size;
}

Segment::Segment(size_t size)
  : _pos{start()}
  , _end{start() + (size - sizeof(Segment))}
{
}

bool
Segment::is_empty() const
{
  return _pos == start();
}

bool
Segment::is_full() const
{
  return _pos == _end;
}

size_t
Segment::size() const
{
  return _end - start();
}

size_t
Segment::occupancy() const
{
  return _pos - start();
}

size_t
Segment::remaining() const
{
  return _end - _pos;
}

void
Segment::reset()
{
  _pos = start();
}

Object*
Segment::append(const Key& key, const Blob& blob)
{
  size_t object_size = Object::size_of(key, blob);
  size_t remaining = _end - _pos;
  if (remaining >= object_size) {
    Object* object = new (_pos) Object(key, blob);
    _pos += object_size;
    return object;
  }
  return nullptr;
}

Object*
Segment::first_object()
{
  if (is_empty()) {
    return nullptr;
  }
  return reinterpret_cast<Object*>(start());
}

Object*
Segment::next_object(Object* object)
{
  char* next = reinterpret_cast<char*>(object) + object->size();
  if (next >= _pos) {
    return nullptr;
  }
  return reinterpret_cast<Object*>(next);
}

char*
Segment::start()
{
  return reinterpret_cast<char*>(this) + sizeof(Segment);
}

const char*
Segment::start() const
{
  return reinterpret_cast<const char*>(this) + sizeof(Segment);
}

Log::Log(const LogConfig& config)
  : _config{config}
{
  auto seg_size = _config.segment_size;
  auto mem_ptr = _config.memory_ptr;
  auto mem_size = _config.memory_size;
  for (size_t seg_off = 0; seg_off < mem_size; seg_off += seg_size) {
    char* seg_ptr = mem_ptr + seg_off;
    Segment* seg = new (seg_ptr) Segment(seg_size);
    _segment_ring.emplace_back(seg);
  }
}

std::optional<Blob>
Log::find(const Key& key) const
{
  const auto& search = _index.find(key);
  if (search) {
    return search.value()->blob();
  }
  return std::nullopt;
}

bool
Log::append(const Key& key, const Blob& blob)
{
  size_t object_size = Object::size_of(key, blob);
  if (object_size > _config.segment_size) {
    return false;
  }
restart:
  if (try_to_append(key, blob)) {
    return true;
  }
  if (expire(object_size) >= object_size) {
    goto restart;
  }
  return false;
}

bool
Log::try_to_append(const Key& key, const Blob& blob)
{
  if (try_to_append(_segment_ring[_segment_ring_tail], key, blob)) {
    return true;
  }
  auto next_tail = _segment_ring_tail + 1;
  if (next_tail == _segment_ring.size()) {
    next_tail = 0;
  }
  if (next_tail == _segment_ring_head) {
    /* Out of clean segments */
    return false;
  }
  _segment_ring_tail = next_tail;
  return try_to_append(_segment_ring[_segment_ring_tail], key, blob);
}

bool
Log::try_to_append(Segment* segment, const Key& key, const Blob& blob)
{
  Object* object = segment->append(key, blob);
  if (!object) {
    return false;
  }
  auto old = _index.insert_or_assign(object->key(), object);
  if (old) {
    old.value()->expire();
  }
  return true;
}

bool
Log::remove(const Key& key)
{
  auto value_opt = _index.find(key);
  if (value_opt) {
    value_opt.value()->expire();
    _index.erase(key);
    return true;
  }
  return false;
}

size_t
Log::expire(size_t reclaim_target)
{
  size_t nr_reclaimed = 0;
  for (;;) {
    if (_segment_ring_head == _segment_ring_tail) {
      /* No more segments to expire */
      break;
    }
    nr_reclaimed += expire(_segment_ring[_segment_ring_head]);
    _segment_ring_head++;
    if (_segment_ring_head == _segment_ring.size()) {
      _segment_ring_head = 0;
    }
    if (nr_reclaimed >= reclaim_target) {
      break;
    }
  }
  return nr_reclaimed;
}

size_t
Log::expire(Segment* seg)
{
  Object* obj = seg->first_object();
  while (obj) {
    if (!obj->is_expired()) {
      _index.erase(obj->key());
    }
    obj = seg->next_object(obj);
  }
  size_t nr_reclaimed = seg->size();
  seg->reset();
  return nr_reclaimed;
}

template<typename T>
static inline int
fls(T x)
{
  return std::numeric_limits<T>::digits - __builtin_clz(x);
}

size_t
Log::segment_index(size_t size)
{
  return fls(size);
}
}
