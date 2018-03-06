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

#include <string_view>

%%{

machine memcache_protocol;

access _fsm_;

action key_start {
    _key_start = p;
}

action key_end {
    _key_end = p;
}

action blob_start {
    _blob_start = p;
}

crlf = "\r\n";

key = [^ ]+ >key_start %key_end;

number = digit+ >{ _number = 0; } ${ _number *= 10; _number += fc - '0'; };

flags = number %{ _flags = _number; };

exptime = number %{ _expiration = _number; };

bytes = number %{ _blob_size = _number; };

set = "set" space key space flags space exptime space bytes space? crlf >blob_start @{ _state = State::CmdSet; };

get = "get" space key crlf @{ _state = State::CmdGet; };

main := (set | get);

}%%

namespace sphinx::memcache {

%% write data nofinal noprefix;

class Parser
{
  int _fsm_cs;

public:
  enum class State
  {
    Error,
    CmdSet,
    CmdGet,
  };

  State _state = State::Error;
  const char* _key_start = nullptr;
  const char* _key_end = nullptr;
  uint64_t _number = 0;
  uint64_t _flags = 0;
  uint64_t _expiration = 0;
  const char* _blob_start = nullptr;
  uint64_t _blob_size = 0;

  Parser()
  {
    %% write init;
  }

  std::string_view key() const
  {
    std::string_view::size_type key_size = _key_end - _key_start;
    return std::string_view{_key_start, key_size};
  }

  size_t parse(std::string_view msg)
  {
    auto* start = msg.data();
    auto* end = start + msg.size();
    auto* next = parse(start, end);
    if (start != next) {
      return next - start;
    }
    return end - start;
  }

private:
  const char* parse(const char *p, const char *pe)
  {
    %% write exec;
    return p;
  }
};

}
