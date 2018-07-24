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

#include <optional>
#include <unordered_map>

namespace sphinx::index {

template<typename Key, typename Value>
class Index
{
  std::unordered_map<Key, Value> _index;

public:
  std::optional<Value> find(Key key) const
  {
    auto it = _index.find(key);
    if (it != _index.end()) {
      return it->second;
    }
    return std::nullopt;
  }
  std::optional<Value> insert_or_assign(Key key, Value value)
  {
    auto [it, inserted] = _index.insert_or_assign(key, value);
    if (!inserted) {
      return it->second;
    }
    return std::nullopt;
  }
  void erase(Key key)
  {
    _index.erase(key);
  }
};
}
