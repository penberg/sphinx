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

#include <sphinx/reactor-epoll.h>

#include <signal.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <stdexcept>
#include <system_error>

namespace sphinx::reactor {

EpollReactor::EpollReactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn)
  : Reactor{thread_id, nr_threads, std::move(on_message_fn)}
  , _epollfd{::epoll_create1(0)}
{
}

EpollReactor::~EpollReactor()
{
  ::close(_epollfd);
}

void
EpollReactor::accept(std::unique_ptr<TcpListener>&& listener)
{
  epoll_event ev = {};
  ev.data.ptr = reinterpret_cast<void*>(listener.get());
  ev.events = EPOLLIN;
  if (epoll_ctl(_epollfd, EPOLL_CTL_ADD, listener->sockfd(), &ev) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  _tcp_listeners.emplace_back(std::move(listener));
}

void
EpollReactor::recv(std::shared_ptr<Socket>&& socket)
{
  epoll_event ev = {};
  ev.data.ptr = reinterpret_cast<void*>(socket.get());
  ev.events = EPOLLIN;
  if (epoll_ctl(_epollfd, EPOLL_CTL_ADD, socket->sockfd(), &ev) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  _sockets.emplace(std::move(socket));
}

void
EpollReactor::close(std::shared_ptr<Socket> socket)
{
  if (::epoll_ctl(_epollfd, EPOLL_CTL_DEL, socket->sockfd(), nullptr) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  if (::shutdown(socket->sockfd(), SHUT_RDWR) < 0) {
    if (errno != ENOTCONN) {
      throw std::system_error(errno, std::system_category(), "close");
    }
  }
  _sockets.erase(socket);
}

void
EpollReactor::run()
{
  std::array<epoll_event, 128> events;
  for (;;) {
    wake_up_pending();
    int nr_events = 0;
    if (poll_messages()) {
      // We had messages, speculate that there's more work in sockets:
      nr_events = ::epoll_wait(_epollfd, events.data(), events.size(), 0);
    } else {
      // No messages, attempt to sleep:
      _thread_is_sleeping[_thread_id].store(true, std::memory_order_seq_cst);
      if (has_messages()) {
        // Raced with producers, restart:
        _thread_is_sleeping[_thread_id].store(false, std::memory_order_seq_cst);
        continue;
      }
      sigset_t sigmask;
      sigemptyset(&sigmask);
      nr_events = ::epoll_pwait(_epollfd, events.data(), events.size(), -1, &sigmask);
      _thread_is_sleeping[_thread_id].store(false, std::memory_order_seq_cst);
    }
    if (nr_events == -1 && errno == EINTR) {
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
  }
}
}
