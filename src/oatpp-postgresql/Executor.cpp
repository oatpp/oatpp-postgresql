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

#include "Executor.hpp"

#include "oatpp/core/parser/ParsingError.hpp"

namespace oatpp { namespace postgresql {

data::share::StringTemplate::Variable Executor::parseIdentifier(parser::Caret& caret) {
  StringTemplate::Variable result;
  result.posStart = caret.getPosition();
  if(caret.canContinueAtChar(':', 1)) {
    auto label = caret.putLabel();
    while(caret.canContinue()) {
      v_char8 a = *caret.getCurrData();
      bool isAllowedChar = (a >= 'a' && a <= 'z') || (a >= 'A' && a <= 'Z') || (a >= '0' && a <= '9') || (a == '_');
      if(!isAllowedChar) {
        result.posEnd = caret.getPosition() - 1;
        result.name = label.toString();
        return result;
      }
      caret.inc();
    }
    result.name = label.toString();
  } else {
    caret.setError("Invalid identifier");
  }
  result.posEnd = caret.getPosition() - 1;
  return result;
}

void Executor::skipStringInQuotes(parser::Caret& caret) {

  bool opened = false;
  while(caret.canContinueAtChar('\'', 1)) {
    opened = true;
    if(caret.findChar('\'')) {
      caret.inc();
      opened = false;
    }
  }

  if(opened) {
    caret.setError("Invalid quote-enclosed string");
  }

}

void Executor::skipStringInDollars(parser::Caret& caret) {

  if(caret.canContinueAtChar('$', 1)) {

    auto label = caret.putLabel();
    if(!caret.findChar('$')) {
      caret.setError("Invalid dollar-enclosed string");
      return;
    }
    caret.inc();
    auto term = label.toString(false);

    while(caret.canContinue()) {

      if(caret.findChar('$')) {
        caret.inc();
        if(caret.isAtText(term->getData(), term->getSize(), true)) {
          return;
        }
      }

    }

  }

  caret.setError("Invalid dollar-enclosed string");

}

data::share::StringTemplate Executor::parseQueryTemplate(const oatpp::String& text) {

  std::vector<StringTemplate::Variable> variables;

  parser::Caret caret(text);
  while(caret.canContinue()) {

    v_char8 c = *caret.getCurrData();

    switch(c) {

      case ':': {
        auto var = parseIdentifier(caret);
        if(var.name) {
          variables.push_back(var);
        }
      }
      break;

      case '\'': skipStringInQuotes(caret); break;
      case '$': skipStringInDollars(caret); break;

      default:
        caret.inc();

    }

  }

  if(caret.hasError()) {
    throw oatpp::parser::ParsingError(caret.getErrorMessage(), caret.getErrorCode(), caret.getPosition());
  }

  return StringTemplate(text, std::move(variables));

}

db::QueryResult Executor::execute(const StringTemplate& queryTemplate, const std::unordered_map<oatpp::String, oatpp::Any>& params) {
  std::unordered_map<oatpp::String, oatpp::String> map;
  for(auto p : params) {
    map[p.first] = "<" + p.first + ">";
  }
  auto res = queryTemplate.format(map);

  OATPP_LOGD("AAA", "query={%s}", res->c_str());

  return db::QueryResult();
}

}}
