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

#include "Connection.hpp"

#include "mapping/Serializer.hpp"
#include "mapping/TypeMapper.hpp"

#include "oatpp/database/Executor.hpp"
#include "oatpp/core/parser/Caret.hpp"

namespace oatpp { namespace postgresql {

class Executor : public database::Executor {
private:
  std::unique_ptr<Oid[]> getParamTypes(const StringTemplate& queryTemplate, const ParamsTypeMap& paramsTypeMap);
  void prepareQuery(const StringTemplate& queryTemplate, const std::shared_ptr<postgresql::Connection>& connection);
  void executeQuery(const StringTemplate& queryTemplate,
                    const std::unordered_map<oatpp::String, oatpp::Void>& params,
                    const std::shared_ptr<postgresql::Connection>& connection);
private:
  mapping::TypeMapper m_typeMapper;
  mapping::Serializer m_serializer;
public:

  StringTemplate parseQueryTemplate(const oatpp::String& name,
                                    const oatpp::String& text,
                                    const ParamsTypeMap& paramsTypeMap) override;

  std::shared_ptr<database::Connection> getConnection() override;

  database::QueryResult execute(const StringTemplate& queryTemplate,
                                const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                const std::shared_ptr<database::Connection>& connection) override;

};

}}

#endif // oatpp_postgresql_Executor_hpp
