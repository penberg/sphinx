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

#include <sphinx/spsc_queue.h>

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
  ~TcpSocket();
  void set_tcp_nodelay(bool nodelay);
  void send(const char* msg, size_t len);
  int sockfd() const;
  void on_read_event() override;

private:
  void recv();
};

using OnMessageFn = std::function<void(void*)>;

constexpr int max_nr_threads = 64;

class Reactor
{
  static pthread_t _pthread_ids[max_nr_threads];
  static std::atomic<bool> _thread_is_sleeping[max_nr_threads];
  static sphinx::spsc::Queue<void*, 256> _msg_queues[max_nr_threads][max_nr_threads];

  size_t _thread_id;
  size_t _nr_threads;
  int _epollfd;
  std::vector<std::unique_ptr<TcpListener>> _tcp_listeners;
  std::set<std::shared_ptr<TcpSocket>> _tcp_sockets;
  OnMessageFn _on_message_fn;

public:
  Reactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn);
  ~Reactor();
  size_t thread_id() const;
  size_t nr_threads() const;
  bool send_msg(size_t thread, void* data);
  void accept(std::unique_ptr<TcpListener>&& listener);
  void recv(std::shared_ptr<TcpSocket>&& socket);
  void close(std::shared_ptr<TcpSocket> socket);
  void run();

private:
  void wake_up(size_t thread_id);
  bool has_messages() const;
  bool poll_messages();
};
}
