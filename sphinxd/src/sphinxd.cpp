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

#include <sphinx/logmem.h>
#include <sphinx/memory.h>
#include <sphinx/protocol.h>
#include <sphinx/reactor.h>

#include <iostream>

#include <getopt.h>
#include <libgen.h>
#include <netinet/in.h>
#include <sys/mman.h>

#include "version.h"

static std::string program;

static constexpr int DEFAULT_PORT = 11211;
static constexpr const char* DEFAULT_LISTEN_ADDR = "0.0.0.0";
static constexpr int DEFAULT_MEMORY_LIMIT = 64;
static constexpr int DEFAULT_SEGMENT_SIZE = 2;
static constexpr int DEFAULT_LISTEN_BACKLOG = 1024;

struct Args
{
  std::string listen_addr = DEFAULT_LISTEN_ADDR;
  int tcp_port = DEFAULT_PORT;
  int memory_limit = DEFAULT_MEMORY_LIMIT; /* in MB */
  int segment_size = DEFAULT_SEGMENT_SIZE; /* in MB */
  int listen_backlog = DEFAULT_LISTEN_BACKLOG;
};

class Buffer
{
  std::vector<char> _data;

public:
  bool is_empty() const;
  void append(std::string_view data);
  void remove_prefix(size_t n);
  std::string_view string_view() const;
};

bool
Buffer::is_empty() const
{
  return _data.empty();
}

void
Buffer::append(std::string_view data)
{
  _data.insert(_data.end(), data.data(), data.data() + data.size());
}

void
Buffer::remove_prefix(size_t n)
{
  _data.erase(_data.begin(), _data.begin() + n);
}

std::string_view
Buffer::string_view() const
{
  return std::string_view{_data.data(), _data.size()};
}

struct Connection
{
  Buffer _rx_buffer;
};

class Server
{
  sphinx::reactor::Reactor _reactor;
  sphinx::logmem::Log _log;

public:
  Server(const sphinx::logmem::LogConfig& log_cfg);
  void serve(const Args& args);

private:
  void accept(int sockfd);
  void recv(Connection& conn,
            std::shared_ptr<sphinx::reactor::TcpSocket> sock,
            std::string_view msg);
  size_t process_one(std::shared_ptr<sphinx::reactor::TcpSocket> sock, std::string_view msg);
};

Server::Server(const sphinx::logmem::LogConfig& log_cfg)
  : _log{log_cfg}
{
}

void
Server::serve(const Args& args)
{
  auto accept_fn = [this](int sockfd) { this->accept(sockfd); };
  auto listener = sphinx::reactor::make_tcp_listener(
    args.listen_addr, args.tcp_port, args.listen_backlog, std::move(accept_fn));
  _reactor.accept(std::move(listener));
  _reactor.run();
}

void
Server::accept(int sockfd)
{
  Connection conn;
  auto recv_fn = [ this, conn = std::move(conn) ](
    const std::shared_ptr<sphinx::reactor::TcpSocket>& sock, std::string_view msg) mutable
  {
    this->recv(conn, sock, msg);
  };
  auto sock = std::make_shared<sphinx::reactor::TcpSocket>(sockfd, std::move(recv_fn));
  sock->set_tcp_nodelay(true);
  this->_reactor.recv(std::move(sock));
}

void
Server::recv(Connection& conn,
             std::shared_ptr<sphinx::reactor::TcpSocket> sock,
             std::string_view msg)
{
  if (msg.size() == 0) {
    _reactor.close(sock);
    return;
  }
  size_t nr_consumed_total = 0;
  if (!conn._rx_buffer.is_empty()) {
    conn._rx_buffer.append(msg);
    msg = conn._rx_buffer.string_view();
  }
  for (;;) {
    if (msg.find('\n') == std::string_view::npos) {
      conn._rx_buffer.append(msg);
      break;
    }
    size_t nr_consumed = process_one(sock, msg);
    if (!nr_consumed) {
      conn._rx_buffer.append(msg);
      break;
    }
    nr_consumed_total += nr_consumed;
    msg.remove_prefix(nr_consumed);
    if (!conn._rx_buffer.is_empty()) {
      conn._rx_buffer.remove_prefix(nr_consumed);
    }
  }
}

size_t
Server::process_one(std::shared_ptr<sphinx::reactor::TcpSocket> sock, std::string_view msg)
{
  using namespace sphinx::memcache;
  Parser parser;
  size_t nr_consumed = parser.parse(msg);
  switch (parser._state) {
    case Parser::State::Error: {
      static std::string response{"ERROR\r\n"};
      sock->send(response.c_str(), response.size());
      break;
    }
    case Parser::State::CmdSet: {
      size_t data_block_size = parser._blob_size + 2;
      if (msg.size() < data_block_size) {
        nr_consumed = 0;
        break;
      }
      nr_consumed += data_block_size;
      std::string_view blob{parser._blob_start, parser._blob_size};
      if (this->_log.append(parser.key(), blob)) {
        static std::string response{"STORED\r\n"};
        sock->send(response.c_str(), response.size());
      } else {
        static std::string response{"SERVER_ERROR out of memory storing object\r\n"};
        sock->send(response.c_str(), response.size());
      }
      break;
    }
    case Parser::State::CmdGet: {
      const auto& key = parser.key();
      auto search = this->_log.find(key);
      std::string response;
      if (search) {
        const auto& value = search.value();
        response += "VALUE ";
        response += key;
        response += " 0 ";
        response += std::to_string(value.size());
        response += "\r\n";
        response += value;
        response += "\r\n";
      }
      response += "END\r\n";
      sock->send(response.c_str(), response.size());
      break;
    }
  }
  return nr_consumed;
}

static void
print_version()
{
  std::cout << "Sphinx " << SPHINX_VERSION << std::endl;
}

static void
print_usage()
{
  std::cout << "Usage: " << program << " [OPTION]..." << std::endl;
  std::cout << "Start the Sphinx daemon." << std::endl;
  std::cout << std::endl;
  std::cout << "Options:" << std::endl;
  std::cout << "  -p, --port number           TCP port to listen to (default: " << DEFAULT_PORT
            << ")" << std::endl;
  std::cout << "  -l, --listen address        interface to listen to (default: "
            << DEFAULT_LISTEN_ADDR << ")" << std::endl;
  std::cout << "  -m, --memory-limit number   Memory limit in MB (default: " << DEFAULT_MEMORY_LIMIT
            << ")" << std::endl;
  std::cout << "  -s, --segment-size number   Segment size in MB (default: " << DEFAULT_SEGMENT_SIZE
            << ")" << std::endl;
  std::cout << "  -b, --listen-backlog number Listen backlog size (default: "
            << DEFAULT_LISTEN_BACKLOG << ")" << std::endl;
  std::cout << "      --help                  print this help text and exit" << std::endl;
  std::cout << "      --version               print Sphinx version and exit" << std::endl;
  std::cout << std::endl;
}

static void
print_opt_error(const std::string& option, const std::string& reason)
{
  std::cerr << program << ": " << reason << " '" << option << "' option" << std::endl;
  std::cerr << "Try '" << program << " --help' for more information" << std::endl;
}

static void
print_unrecognized_opt(const std::string& option)
{
  print_opt_error(option, "unregonized");
}

static Args
parse_cmd_line(int argc, char* argv[])
{
  static struct option long_options[] = {{"port", required_argument, 0, 'p'},
                                         {"listen", required_argument, 0, 'l'},
                                         {"memory-limit", required_argument, 0, 'm'},
                                         {"segment-size", required_argument, 0, 's'},
                                         {"listen-backlog", required_argument, 0, 'b'},
                                         {"help", no_argument, 0, 'h'},
                                         {"version", no_argument, 0, 'v'},
                                         {0, 0, 0, 0}};
  Args args;
  int opt, long_index;
  while ((opt = ::getopt_long(argc, argv, "U:l:m:s:b:", long_options, &long_index)) != -1) {
    switch (opt) {
      case 'p':
        args.tcp_port = std::stoi(optarg);
        break;
      case 'l':
        args.listen_addr = optarg;
        break;
      case 'm':
        args.memory_limit = std::stoi(optarg);
        break;
      case 's':
        args.segment_size = std::stoi(optarg);
        break;
      case 'b':
        args.listen_backlog = std::stoi(optarg);
        break;
      case 'h':
        print_usage();
        std::exit(EXIT_SUCCESS);
      case 'v':
        print_version();
        std::exit(EXIT_SUCCESS);
      case '?':
        print_unrecognized_opt(argv[optind - 1]);
        std::exit(EXIT_FAILURE);
      default:
        print_usage();
        std::exit(EXIT_FAILURE);
    }
  }
  return args;
}

int
main(int argc, char* argv[])
{
  try {
    program = ::basename(argv[0]);
    auto args = parse_cmd_line(argc, argv);
    size_t mem_size = args.memory_limit * 1024 * 1024;
    sphinx::memory::Memory memory = sphinx::memory::Memory::mmap(mem_size);
    sphinx::logmem::LogConfig log_cfg;
    log_cfg.segment_size = args.segment_size * 1024 * 1024;
    log_cfg.memory_ptr = reinterpret_cast<char*>(memory.addr());
    log_cfg.memory_size = memory.size();
    Server server{log_cfg};
    server.serve(args);
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << std::endl;
    std::exit(EXIT_FAILURE);
  }
  std::exit(EXIT_SUCCESS);
}
