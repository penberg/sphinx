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

#include <gtest/gtest.h>

#include <sphinx/string.h>

#include <cstdlib>
#include <string>

TEST(StringTest, to_string)
{
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(std::to_string(i), sphinx::to_string(i));
    auto v = std::rand();
    ASSERT_EQ(std::to_string(v), sphinx::to_string(v));
  }
}
