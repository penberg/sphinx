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

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace sphinx::reactor {

TcpListener::TcpListener(int sockfd, TcpAcceptFn&& accept_fn)
  : _sockfd{sockfd}
  , _accept_fn{accept_fn}
{
}

TcpListener::~TcpListener()
{
  ::close(_sockfd);
}

void
TcpListener::on_read_event()
{
  accept();
}

void
TcpListener::accept()
{
  int connfd = ::accept4(_sockfd, nullptr, nullptr, SOCK_NONBLOCK);
  if (connfd < 0) {
    throw std::system_error(errno, std::system_category(), "accept4");
  }
  _accept_fn(connfd);
}

int
TcpListener::sockfd() const
{
  return _sockfd;
}

std::unique_ptr<TcpListener>
make_tcp_listener(const std::string& iface, int port, int backlog, TcpAcceptFn&& accept_fn)
{
  addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = 0;
  hints.ai_flags = AI_PASSIVE | AI_ADDRCONFIG;
  addrinfo* addresses = nullptr;
  int err = getaddrinfo(iface.c_str(), std::to_string(port).c_str(), &hints, &addresses);
  if (err != 0) {
    throw std::runtime_error("'" + iface + "': " + gai_strerror(err));
  }
  for (addrinfo* rp = addresses; rp != NULL; rp = rp->ai_next) {
    int sockfd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sockfd < 0) {
      continue;
    }
    int one = 1;
    ::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &one, sizeof(one));
    if (::bind(sockfd, rp->ai_addr, rp->ai_addrlen) < 0) {
      ::close(sockfd);
      continue;
    }
    if (::listen(sockfd, backlog) < 0) {
      ::close(sockfd);
      continue;
    }
    freeaddrinfo(addresses);
    return std::make_unique<TcpListener>(sockfd, std::move(accept_fn));
  }
  freeaddrinfo(addresses);
  throw std::runtime_error("Failed to listen to interface: '" + iface + "'");
}

TcpSocket::TcpSocket(int sockfd, TcpRecvFn&& recv_fn)
  : _sockfd{sockfd}
  , _recv_fn{recv_fn}
{
}

TcpSocket::~TcpSocket()
{
  ::close(_sockfd);
}

void
TcpSocket::set_tcp_nodelay(bool nodelay)
{
  int value = nodelay;
  if (setsockopt(_sockfd, SOL_TCP, TCP_NODELAY, &value, sizeof(value)) < 0) {
    throw std::system_error(errno, std::system_category(), "setsockopt");
  }
}

void
TcpSocket::send(const char* msg, size_t len)
{
  ssize_t nr = ::send(_sockfd, msg, len, MSG_NOSIGNAL | MSG_DONTWAIT);
  if ((nr < 0) && (errno == ECONNRESET || errno == EPIPE)) {
    return;
  }
  if (nr < 0) {
    throw std::system_error(errno, std::system_category(), "send");
  }
  if (size_t(nr) != len) {
    throw std::runtime_error("partial send");
  }
}

int
TcpSocket::sockfd() const
{
  return _sockfd;
}

void
TcpSocket::on_read_event()
{
  constexpr size_t rx_buf_size = 256 * 1024;
  std::array<char, rx_buf_size> rx_buf;
  ssize_t nr = ::recv(_sockfd, rx_buf.data(), rx_buf.size(), MSG_DONTWAIT);
  if ((nr < 0 && errno == ECONNRESET)) {
    _recv_fn(this->shared_from_this(), std::string_view{});
    return;
  }
  if (nr < 0) {
    throw std::system_error(errno, std::system_category(), "recv");
  }
  _recv_fn(this->shared_from_this(),
           std::string_view{rx_buf.data(), std::string_view::size_type(nr)});
}

void
Reactor::accept(std::unique_ptr<TcpListener>&& listener)
{
  _tcp_listeners.emplace_back(std::move(listener));
}

void
Reactor::recv(std::shared_ptr<TcpSocket>&& socket)
{
  epoll_event ev = {};
  ev.data.ptr = reinterpret_cast<void*>(socket.get());
  ev.events = EPOLLIN;
  if (epoll_ctl(_epollfd, EPOLL_CTL_ADD, socket->sockfd(), &ev) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  _tcp_sockets.emplace(std::move(socket));
}

pthread_t Reactor::_pthread_ids[max_nr_threads];
std::atomic<bool> Reactor::_thread_is_sleeping[max_nr_threads];
sphinx::spsc::Queue<void*, 256> Reactor::_msg_queues[max_nr_threads][max_nr_threads];

void
handler(int sig, siginfo_t* siginfo, void* data)
{
}

Reactor::Reactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn)
  : _thread_id{thread_id}
  , _nr_threads{nr_threads}
  , _on_message_fn{on_message_fn}
{
  sigset_t mask;
  sigemptyset(&mask);
  struct sigaction sa;
  sa.sa_sigaction = handler;
  sa.sa_mask = mask;
  sa.sa_flags = SA_SIGINFO | SA_RESTART;
  if (::sigaction(SIGUSR1, &sa, nullptr) < 0) {
    throw std::system_error(errno, std::system_category(), "sigaction");
  }

  sigaddset(&mask, SIGUSR1);
  if (::pthread_sigmask(SIG_BLOCK, &mask, NULL) < 0) {
    throw std::system_error(errno, std::system_category(), "pthread_sigmask");
  }
  _thread_is_sleeping[_thread_id].store(false, std::memory_order_seq_cst);
  _pthread_ids[_thread_id] = pthread_self();
  _epollfd = ::epoll_create1(0);
}

Reactor::~Reactor()
{
  ::close(_epollfd);
}

size_t
Reactor::thread_id() const
{
  return _thread_id;
}

size_t
Reactor::nr_threads() const
{
  return _nr_threads;
}

bool
Reactor::send_msg(size_t remote_id, void* msg)
{
  if (remote_id == _thread_id) {
    throw std::invalid_argument("Attempting to send message to self");
  }
  auto& queue = _msg_queues[remote_id][_thread_id];
  if (!queue.try_to_emplace(msg)) {
    return false;
  }
  if (_thread_is_sleeping[remote_id].load(std::memory_order_seq_cst)) {
    wake_up(remote_id);
  }
  return true;
}

void
Reactor::close(std::shared_ptr<TcpSocket> socket)
{
  if (::epoll_ctl(_epollfd, EPOLL_CTL_DEL, socket->sockfd(), nullptr) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  if (::shutdown(socket->sockfd(), SHUT_RDWR) < 0) {
    if (errno != ENOTCONN) {
      throw std::system_error(errno, std::system_category(), "close");
    }
  }
  _tcp_sockets.erase(socket);
}

void
Reactor::run()
{
  std::array<epoll_event, 128> events;
  for (auto& listener : _tcp_listeners) {
    epoll_event ev = {};
    ev.data.ptr = reinterpret_cast<void*>(listener.get());
    ev.events = EPOLLIN;
    if (::epoll_ctl(_epollfd, EPOLL_CTL_ADD, listener->sockfd(), &ev) < 0) {
      throw std::system_error(errno, std::system_category(), "epoll_ctl");
    }
  }
  for (;;) {
    _thread_is_sleeping[_thread_id].store(true, std::memory_order_seq_cst);
    poll_messages();
    sigset_t sigmask;
    sigemptyset(&sigmask);
    int nr_events = ::epoll_pwait(_epollfd, events.data(), events.size(), -1, &sigmask);
    _thread_is_sleeping[_thread_id].store(false, std::memory_order_seq_cst);
    if (nr_events == -1 && errno == EINTR) {
      poll_messages();
      continue;
    }
    if (nr_events < 0) {
      throw std::system_error(errno, std::system_category(), "epoll_wait");
    }
    for (int i = 0; i < nr_events; i++) {
      epoll_event* event = &events[i];
      auto* listener = reinterpret_cast<Evented*>(event->data.ptr);
      if (listener) {
        listener->on_read_event();
      }
    }
    poll_messages();
  }
}

void
Reactor::wake_up(size_t thread_id)
{
  ::pthread_kill(_pthread_ids[thread_id], SIGUSR1);
}

void
Reactor::poll_messages()
{
  for (size_t other = 0; other < _nr_threads; other++) {
    if (other == _thread_id) {
      continue;
    }
    auto& queue = _msg_queues[_thread_id][other];
    for (;;) {
      auto* msg = queue.front();
      if (!msg) {
        break;
      }
      _on_message_fn(*msg);
      queue.pop();
    }
  }
}
}
