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

#include <sphinx/spsc_queue.h>
#include <thread>

TEST(QueueTest, try_to_emplace)
{
  using namespace sphinx::spsc;
  Queue<int, 128> queue;
  ASSERT_TRUE(queue.empty());
  ASSERT_TRUE(queue.try_to_emplace(1));
  ASSERT_FALSE(queue.empty());
}

TEST(QueueTest, producer_consumer)
{
  using namespace sphinx::spsc;
  constexpr int nr_iterations = 1000000;
  Queue<int, 128> queue;
  std::thread producer{[&queue]() {
    for (int i = 0; i < nr_iterations; i++) {
      for (;;) {
        if (queue.try_to_emplace(i)) {
          break;
        }
      }
    }
  }};
  std::thread consumer{[&queue]() {
    for (int i = 0; i < nr_iterations; i++) {
      for (;;) {
        auto* item = queue.front();
        if (item) {
          ASSERT_EQ(i, *item);
          queue.pop();
          break;
        }
      }
    }
  }};
  producer.join();
  consumer.join();
}
