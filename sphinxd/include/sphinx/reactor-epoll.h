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

#include <sphinx/reactor.h>

namespace sphinx::reactor {

class EpollReactor : public Reactor
{
  std::vector<std::unique_ptr<TcpListener>> _tcp_listeners;
  std::set<std::shared_ptr<Socket>> _sockets;
  int _epollfd;

public:
  EpollReactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn);
  ~EpollReactor();
  virtual void accept(std::unique_ptr<TcpListener>&& listener) override;
  virtual void recv(std::shared_ptr<Socket>&& socket) override;
  virtual void close(std::shared_ptr<Socket> socket) override;
  virtual void run() override;
};
}
