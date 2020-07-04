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

#ifndef oatpp_postgresql_Executor_hpp
#define oatpp_postgresql_Executor_hpp

#include "oatpp/db/Executor.hpp"
#include "oatpp/core/parser/Caret.hpp"

namespace oatpp { namespace postgresql {

class Executor : public db::Executor {
private:
  static StringTemplate::Variable parseIdentifier(parser::Caret& caret);
  static void skipStringInQuotes(parser::Caret& caret);
  static void skipStringInDollars(parser::Caret& caret);
public:

  StringTemplate parseQueryTemplate(const oatpp::String& text) override;

  db::QueryResult execute(const StringTemplate& queryTemplate, const std::unordered_map<oatpp::String, oatpp::Any>& params) override;

};

}}

#endif // oatpp_postgresql_Executor_hpp
