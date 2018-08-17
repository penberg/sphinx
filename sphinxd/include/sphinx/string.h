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

#include <string>

namespace sphinx {

inline std::string
to_string(unsigned long n)
{
  if (n != 0) {
    constexpr auto size = 20;
    char ret[size];
    size_t offset = size;
    while (n > 0) {
      auto digit = n % 10;
      n = (n - digit) / 10;
      ret[--offset] = '0' + digit;
    }
    return std::string{ret + offset, size - offset};
  } else {
    return "0";
  }
}
}
