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

#include <sphinx/buffer.h>

namespace sphinx::buffer {

bool
Buffer::is_empty() const
{
  return _data.empty();
}

void
Buffer::append(std::string_view data)
{
  _data.insert(_data.end(), data.data(), data.data() + data.size());
}

void
Buffer::remove_prefix(size_t n)
{
  _data.erase(_data.begin(), _data.begin() + n);
}

const char*
Buffer::data() const
{
  return _data.data();
}

size_t
Buffer::size() const
{
  return _data.size();
}

std::string_view
Buffer::string_view() const
{
  return std::string_view{_data.data(), _data.size()};
}
}
