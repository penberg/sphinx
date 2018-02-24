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

#include <functional>
#include <memory>
#include <set>
#include <string>

namespace sphinx::reactor {

using TcpAcceptFn = std::function<void(int sockfd)>;

struct Evented
{
  virtual ~Evented() {}
  virtual void on_read_event() = 0;
};

class TcpListener : public Evented
{
  int _sockfd;
  TcpAcceptFn _accept_fn;

public:
  explicit TcpListener(int sockfd, TcpAcceptFn&& accept_fn);
  ~TcpListener();

  int sockfd() const;

  void on_read_event() override;

private:
  void accept();
};

std::unique_ptr<TcpListener>
make_tcp_listener(const std::string& iface, int port, int backlog, TcpAcceptFn&& recv_fn);

class TcpSocket;

using TcpRecvFn = std::function<void(std::shared_ptr<TcpSocket>, std::string_view)>;

class TcpSocket
  : public Evented
  , public std::enable_shared_from_this<TcpSocket>
{
  int _sockfd;
  TcpRecvFn _recv_fn;

public:
  explicit TcpSocket(int sockfd, TcpRecvFn&& recv_fn);
  void set_tcp_nodelay(bool nodelay);
  void send(const char* msg, size_t len);
  int sockfd() const;
  void on_read_event() override;

private:
  void recv();
};

class Reactor
{
  int _epollfd;
  std::vector<std::unique_ptr<TcpListener>> _tcp_listeners;
  std::set<std::shared_ptr<TcpSocket>> _tcp_sockets;

public:
  Reactor();
  ~Reactor();
  void accept(std::unique_ptr<TcpListener>&& listener);
  void recv(std::shared_ptr<TcpSocket>&& socket);
  void close(std::shared_ptr<TcpSocket> socket);
  void run();
};
}
