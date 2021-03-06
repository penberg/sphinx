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

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <stdexcept>
#include <system_error>

namespace sphinx::reactor {

class Eventfd : public Pollable
{
  int _efd;

public:
  explicit Eventfd(int efd)
    : _efd{efd}
  {
  }

  int fd() const override
  {
    return _efd;
  }

  void on_pollin() override
  {
    eventfd_t unused;
    if (::eventfd_read(_efd, &unused) < 0) {
      throw std::system_error(errno, std::system_category(), "eventfd_read");
    }
  }
  bool on_pollout() override
  {
    return false;
  }
};

EpollReactor::EpollReactor(size_t thread_id, size_t nr_threads, OnMessageFn&& on_message_fn)
  : Reactor{thread_id, nr_threads, std::move(on_message_fn)}
  , _epollfd{::epoll_create1(0)}
{
  auto eventfd = std::make_shared<Eventfd>(_efd);
  update_epoll(eventfd.get(), EPOLLIN);
  _pollables.emplace(eventfd->fd(), eventfd);
}

EpollReactor::~EpollReactor()
{
  ::close(_epollfd);
}

void
EpollReactor::accept(std::shared_ptr<TcpListener>&& listener)
{
  update_epoll(listener.get(), EPOLLIN);
  _pollables.emplace(listener->fd(), std::move(listener));
}

void
EpollReactor::recv(std::shared_ptr<Socket>&& socket)
{
  update_epoll(socket.get(), EPOLLIN);
  _pollables.emplace(socket->fd(), std::move(socket));
}

void
EpollReactor::send(std::shared_ptr<Socket> socket)
{
  update_epoll(socket.get(), EPOLLIN | EPOLLOUT);
  _pollables.emplace(socket->fd(), socket);
}

void
EpollReactor::close(std::shared_ptr<Socket> socket)
{
  _epoll_events.erase(socket->fd());
  if (::epoll_ctl(_epollfd, EPOLL_CTL_DEL, socket->fd(), nullptr) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  if (::shutdown(socket->fd(), SHUT_RDWR) < 0) {
    if (errno != ENOTCONN) {
      throw std::system_error(errno, std::system_category(), "close");
    }
  }
  _pollables.erase(socket->fd());
}

void
EpollReactor::run()
{
  std::array<epoll_event, 128> events;
  for (;;) {
    wake_up_pending();
    int nr_events = 0;
    if (poll_messages()) {
      // We had messages, speculate that there will be more message, and
      // therefore do not sleep:
      nr_events = ::epoll_wait(_epollfd, events.data(), events.size(), 0);
    } else {
      // No messages, attempt to sleep:
      _thread_is_sleeping[_thread_id].store(true, std::memory_order_seq_cst);
      if (has_messages()) {
        // Raced with producers, restart:
        _thread_is_sleeping[_thread_id].store(false, std::memory_order_seq_cst);
        continue;
      }
      nr_events = ::epoll_wait(_epollfd, events.data(), events.size(), -1);
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
      auto fd = event->data.fd;
      auto it = _pollables.find(fd);
      if (it == _pollables.end()) {
        ::epoll_ctl(_epollfd, EPOLL_CTL_DEL, fd, nullptr);
        continue;
      }
      auto pollable = it->second;
      if (event->events & EPOLLIN) {
        pollable->on_pollin();
      }
      if (event->events & EPOLLOUT) {
        if (pollable->on_pollout()) {
          update_epoll(pollable.get(), EPOLLIN);
        }
      }
    }
  }
}

void
EpollReactor::update_epoll(Pollable* pollable, uint32_t events)
{
  int op = EPOLL_CTL_ADD;
  auto it = _epoll_events.find(pollable->fd());
  if (it != _epoll_events.end()) {
    if (events == it->second) {
      return;
    }
    op = EPOLL_CTL_MOD;
  }
  ::epoll_event ev = {};
  ev.data.fd = pollable->fd();
  ev.events = events;
  if (::epoll_ctl(_epollfd, op, pollable->fd(), &ev) < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_ctl");
  }
  _epoll_events.insert_or_assign(pollable->fd(), events);
}
}
