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

#include <gtest/gtest.h>

#include <sphinx/protocol.h>

TEST(ProtocolTest, parse_error)
{
  using namespace sphinx::memcache;
  std::string msg = "foo";
  Parser parser;
  parser.parse(msg);
  ASSERT_EQ(parser._state, Parser::State::Error);
}

TEST(ProtocolTest, parse_set)
{
  using namespace sphinx::memcache;
  std::string msg = "set foo 0 0 3\r\nbar\r\n";
  Parser parser;
  parser.parse(msg);
  ASSERT_EQ(parser._state, Parser::State::CmdSet);
}

TEST(ProtocolTest, parse_get)
{
  using namespace sphinx::memcache;
  std::string msg = "get foo\r\n";
  Parser parser;
  parser.parse(msg);
  ASSERT_EQ(parser._state, Parser::State::CmdGet);
}

TEST(ProtocolTest, parse_many)
{
  using namespace sphinx::memcache;
  std::string raw_msg = "set foo 0 0 3\r\nbar\r\nget foo\r\n";
  std::string_view msg = raw_msg;
  {
    Parser parser;
    auto nr_consumed = parser.parse(msg);
    ASSERT_EQ(15, nr_consumed);
    ASSERT_EQ(parser._state, Parser::State::CmdSet);
    ASSERT_EQ(3, parser._blob_size);
    msg.remove_prefix(nr_consumed + parser._blob_size + 2);
  }
  {
    Parser parser;
    auto nr_consumed = parser.parse(msg);
    ASSERT_EQ(9, nr_consumed);
    ASSERT_EQ(parser._state, Parser::State::CmdGet);
  }
}
