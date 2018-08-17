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

#include <sphinx/buffer.h>

#include <algorithm>

TEST(BufferTest, append)
{
  using namespace sphinx::buffer;
  Buffer buf;
  ASSERT_TRUE(buf.size() == 0);
  std::string value = "The quick brown fox jumps over the lazy dog";
  buf.append(value);
  ASSERT_EQ(value.size(), buf.size());
  ASSERT_EQ(value, buf.string_view());
}
