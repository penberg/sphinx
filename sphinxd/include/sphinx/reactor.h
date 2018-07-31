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
#include <bitset>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <sys/socket.h>

namespace sphinx::reactor {

struct SockAddr
{
  ::sockaddr_storage addr;
  ::socklen_t len;

  SockAddr(::sockaddr_storage addr, ::socklen_t len);
  SockAddr(const SockAddr&) = default;
  SockAddr(SockAddr&&) = default;
  SockAddr& operator=(const SockAddr&) = default;
};

using TcpAcceptFn = std::function<void(int sockfd)>;

struct Evented
{
  virtual ~Evented() {}
  virtual void on_read_event() = 0;
};

class Socket : public Evented
{
protected:
  int _sockfd;

public:
  explicit Socket(int sockfd);
  virtual ~Socket();

  int sockfd() const;
  virtual void send(const char* msg, size_t len, std::optional<SockAddr> dst = std::nullopt) = 0;
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
  : public Socket
  , public std::enable_shared_from_this<TcpSocket>
{
  TcpRecvFn _recv_fn;

public:
  explicit TcpSocket(int sockfd, TcpRecvFn&& recv_fn);
  ~TcpSocket();
  void set_tcp_nodelay(bool nodelay);
  void send(const char* msg, size_t len, std::optional<SockAddr> dst = std::nullopt) override;
  void on_read_event() override;

private:
  void recv();
};

class UdpSocket;

using UdpRecvFn =
  std::function<void(std::shared_ptr<UdpSocket>, std::string_view, std::optional<SockAddr>)>;

class UdpSocket
  : public Socket
  , public std::enable_shared_from_this<UdpSocket>
{
  UdpRecvFn _recv_fn;

public:
  explicit UdpSocket(int sockfd, UdpRecvFn&& recv_fn);
  ~UdpSocket();
  void send(const char* msg, size_t len, std::optional<SockAddr> dst) override;
  void on_read_event() override;
};

std::shared_ptr<UdpSocket>
make_udp_socket(const std::string& iface, int port, UdpRecvFn&& recv_fn);

using OnMessageFn = std::function<void(void*)>;

constexpr int max_nr_threads = 64;

class Reactor
{
protected:
  static pthread_t _pthread_ids[max_nr_threads];
  static std::atomic<bool> _thread_is_sleeping[max_nr_threads];
  static constexpr int _msg_queue_size = 1024;
  static sphinx::spsc::Queue<void*, _msg_queue_size> _msg_queues[max_nr_threads][max_nr_threads];

  size_t _thread_id;
  size_t _nr_threads;
  std::bitset<max_nr_threads> _pending_wakeups;
  OnMessageFn _on_message_fn;

public:
  static std::string default_backend();

  Reactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn);
  virtual ~Reactor();
  size_t thread_id() const;
  size_t nr_threads() const;
  bool send_msg(size_t thread, void* data);
  virtual void accept(std::unique_ptr<TcpListener>&& listener) = 0;
  virtual void recv(std::shared_ptr<Socket>&& socket) = 0;
  virtual void close(std::shared_ptr<Socket> socket) = 0;
  virtual void run() = 0;

protected:
  void wake_up_pending();
  void wake_up(size_t thread_id);
  bool has_messages() const;
  bool poll_messages();
};

std::unique_ptr<Reactor>
make_reactor(const std::string& backend,
             size_t thread_id,
             size_t nr_threads,
             OnMessageFn&& on_message_fn);
}
