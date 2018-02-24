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

#include <sphinx/logmem.h>

#include <algorithm>

static std::string
make_random(size_t len)
{
  auto make_random_char = []() {
    static const char chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const size_t nr_chars = sizeof(chars) - 1;
    return chars[rand() % nr_chars];
  };
  std::string str(len, 0);
  std::generate_n(str.begin(), len, make_random_char);
  return str;
}

TEST(LogTest, append)
{
  using namespace sphinx::logmem;
  std::array<char, 128> memory;
  LogConfig cfg;
  cfg.segment_size = 64;
  cfg.memory_ptr = memory.data();
  cfg.memory_size = memory.size();
  Log log{cfg};
  auto key = make_random(8);
  auto blob = make_random(16);
  log.append(key, blob);
  auto blob_opt = log.find(key);
  ASSERT_TRUE(blob_opt.has_value());
  ASSERT_EQ(blob_opt.value(), blob);
}

TEST(LogTest, append_full)
{
  using namespace sphinx::logmem;
  std::array<char, 1024> memory;
  LogConfig cfg;
  cfg.segment_size = 64;
  cfg.memory_ptr = memory.data();
  cfg.memory_size = memory.size();
  Log log{cfg};
  std::string key;
  std::string blob;
  for (;;) {
    key = make_random(8);
    blob = make_random(16);
    if (!log.append(key, blob)) {
      break;
    }
  }
  key = make_random(8);
  blob = make_random(16);
  ASSERT_FALSE(log.append(key, blob));
}

TEST(LogTest, compact)
{
  using namespace sphinx::logmem;
  std::array<char, 64> memory;
  LogConfig cfg;
  cfg.segment_size = 64;
  cfg.memory_ptr = memory.data();
  cfg.memory_size = memory.size();
  Log log{cfg};
  auto key = make_random(8);
  auto blob = make_random(16);
  ASSERT_TRUE(log.append(key, blob));
  ASSERT_FALSE(log.append(key, blob));
  ASSERT_TRUE(log.remove(key));
  ASSERT_EQ(log.compact(), Object::size_of(key, blob));
  ASSERT_TRUE(log.append(key, blob));
  auto blob_opt = log.find(key);
  ASSERT_TRUE(blob_opt.has_value());
  ASSERT_EQ(blob_opt.value(), blob);
}
