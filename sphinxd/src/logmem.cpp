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
  _segments.resize(segment_index(_config.segment_size) + 1);
  auto seg_size = _config.segment_size;
  auto mem_ptr = _config.memory_ptr;
  auto mem_size = _config.memory_size;
  for (size_t seg_off = 0; seg_off < mem_size; seg_off += seg_size) {
    char* seg_ptr = mem_ptr + seg_off;
    Segment* seg = new (seg_ptr) Segment(seg_size);
    put_segment(seg);
  }
}

std::optional<Blob>
Log::find(const Key& key) const
{
  const auto& search = _index.find(key);
  if (search != _index.end()) {
    return search->second->blob();
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
  if (append_nocompact(key, blob)) {
    return true;
  }
  if (compact(object_size) >= object_size) {
    goto restart;
  }
  return false;
}

bool
Log::append_nocompact(const Key& key, const Blob& blob)
{
  if (_current_segment) {
    if (try_to_append(_current_segment, key, blob)) {
      return true;
    }
    put_segment(_current_segment);
    _current_segment = nullptr;
  }
  Segment* seg = get_segment();
  if (seg) {
    if (try_to_append(seg, key, blob)) {
      _current_segment = seg;
      return true;
    }
    put_segment(seg);
  }
  return false;
}

bool
Log::try_to_append(Segment* segment, const Key& key, const Blob& blob)
{
  Object* object = segment->append(key, blob);
  if (!object) {
    return false;
  }
  auto[it, inserted] = _index.insert_or_assign(object->key(), object);
  if (!inserted) {
    it->second->expire();
  }
  return true;
}

bool
Log::remove(const Key& key)
{
  auto it = _index.find(key);
  if (it != _index.end()) {
    it->second->expire();
    _index.erase(it);
    return true;
  }
  return false;
}

size_t
Log::compact(size_t reclaim_target)
{
  size_t nr_reclaimed = 0;
  std::list<Segment*> compacted;
  for (size_t i = 0; i < _segments.size(); i++) {
    size_t idx = _segments.size() - i - 1;
    for (auto it = _segments[idx].begin(); it != _segments[idx].end();) {
      Segment* seg = *it;
      it = _segments[idx].erase(it);
      nr_reclaimed += compact(seg);
      compacted.emplace_back(seg);
      if (nr_reclaimed >= reclaim_target) {
        break;
      }
    }
  }
  while (!compacted.empty()) {
    Segment* seg = compacted.front();
    compacted.pop_front();
    put_segment(seg);
  }
  return nr_reclaimed;
}

size_t
Log::compact(Segment* seg)
{
  size_t to_reclaim = 0;
  Object* obj = seg->first_object();
  while (obj) {
    if (obj->is_expired()) {
      to_reclaim += obj->size();
    } else {
      auto it = _index.find(obj->key());
      if (it == _index.end()) {
        to_reclaim += obj->size();
      }
    }
    obj = seg->next_object(obj);
  }
  if (!to_reclaim) {
    return 0;
  }
  obj = seg->first_object();
  while (obj) {
    if (!obj->is_expired()) {
      auto it = _index.find(obj->key());
      if (it != _index.end()) {
        if (!append_nocompact(obj->key(), obj->blob())) {
          return 0;
        }
      }
    }
    obj = seg->next_object(obj);
  }
  size_t nr_reclaimed = seg->occupancy();
  seg->reset();
  return nr_reclaimed;
}

Segment*
Log::get_segment()
{
  for (size_t i = 0; i < _segments.size(); i++) {
    size_t idx = _segments.size() - i - 1;
    auto it = _segments[idx].begin();
    if (it != _segments[idx].end()) {
      Segment* seg = *it;
      _segments[idx].erase(it);
      return seg;
    }
  }
  return nullptr;
}

void
Log::put_segment(Segment* seg)
{
  size_t idx = segment_index(seg->remaining());
  _segments[idx].emplace_back(seg);
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
