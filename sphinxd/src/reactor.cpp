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

#include <sphinx/reactor.h>

namespace sphinx::reactor {

std::unique_ptr<Reactor>
make_reactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn)
{
  return std::make_unique<Reactor>(thread_id, nr_threads, std::move(on_message_fn));
}
}