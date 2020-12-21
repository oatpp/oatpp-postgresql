/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Leonid Stryzhevskyi <lganzzzo@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************************/

#include "ParserTest.hpp"

#include "oatpp-postgresql/ql_template/Parser.hpp"

namespace oatpp { namespace test { namespace postgresql { namespace ql_template {

namespace {

typedef oatpp::postgresql::ql_template::Parser Parser;

}

void ParserTest::onRun() {

//  {
//    oatpp::String text = "";
//    auto result = Parser::preprocess(text);
//    OATPP_ASSERT(result == text);
//  }
//
//  {
//    oatpp::String text = "SELECT * FROM my_table;";
//    auto result = Parser::preprocess(text);
//    OATPP_ASSERT(result == text);
//  }

  {
    oatpp::String text = "SELECT <[ * ]> FROM my_table;";
    auto result = Parser::preprocess(text);
    OATPP_ASSERT(result == "SELECT ___ FROM my_table;");
  }

  {
    oatpp::String text = "<[SELECT * FROM my_table;]>";
    auto result = Parser::preprocess(text);
    OATPP_ASSERT(result == "_______________________");
  }

  {
    oatpp::String text = "SELECT <[ * ]> FROM]> my_table;";
    auto result = Parser::preprocess(text);
    OATPP_LOGD(TAG, "sql='%s'", text->getData());
    OATPP_LOGD(TAG, "res='%s'", result->getData());
  }

  {
    oatpp::String text = "SELECT < [ * ] > FROM]> my_table;";
    auto result = Parser::preprocess(text);
    OATPP_LOGD(TAG, "sql='%s'", text->getData());
    OATPP_LOGD(TAG, "res='%s'", result->getData());
  }

  {
    oatpp::String text = "SELECT <[ * ']>' FROM my_table;";
    auto result = Parser::preprocess(text);
    OATPP_LOGD(TAG, "sql='%s'", text->getData());
    OATPP_LOGD(TAG, "res='%s'", result->getData());
  }

}

}}}}
