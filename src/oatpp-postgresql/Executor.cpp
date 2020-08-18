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

#include "ql_template/Parser.hpp"
#include "ql_template/TemplateValueProvider.hpp"

#include "QueryResult.hpp"

#include "oatpp/core/data/stream/ChunkedBuffer.hpp"

#include <vector>

namespace oatpp { namespace postgresql {

Executor::QueryParams::QueryParams(const StringTemplate& queryTemplate,
                                   const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                   const mapping::TypeMapper& typeMapper,
                                   const mapping::Serializer& serializer)
{

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  query = extra->preparedTemplate->c_str();
  queryName = extra->templateName->c_str();

  count = queryTemplate.getTemplateVariables().size();

  outData.resize(count);
  paramOids.resize(count);
  paramValues.resize(count);
  paramLengths.resize(count);
  paramFormats.resize(count);

  for(v_uint32 i = 0; i < count; i ++) {
    const auto& var = queryTemplate.getTemplateVariables()[i];
    auto it = params.find(var.name);
    if(it == params.end()) {
      throw std::runtime_error("[oatpp::postgresql::Executor::QueryParams::QueryParams()]: "
                               "Error. Parameter not found " + var.name->std_str());
    }

    auto& data = outData[i];
    serializer.serialize(data, it->second);

    paramOids[i] = typeMapper.getTypeOid(it->second.valueType);
    paramValues[i] = data.data;
    paramLengths[i] = data.dataSize;
    paramFormats[i] = data.dataFormat;
  }

}

Executor::Executor(const std::shared_ptr<provider::Provider<Connection>>& connectionProvider)
  : m_connectionProvider(connectionProvider)
  , m_resultMapper(std::make_shared<mapping::ResultMapper>())
{}


std::unique_ptr<Oid[]> Executor::getParamTypes(const StringTemplate& queryTemplate, const ParamsTypeMap& paramsTypeMap) {

  std::unique_ptr<Oid[]> result(new Oid[queryTemplate.getTemplateVariables().size()]);

  for(v_uint32 i = 0; i < queryTemplate.getTemplateVariables().size(); i++) {
    const auto& v = queryTemplate.getTemplateVariables()[i];
    auto it = paramsTypeMap.find(v.name);
    if(it == paramsTypeMap.end()) {
      throw std::runtime_error("[oatpp::postgresql::Executor::getParamTypes()]: Error. "
                               "Type info not found for variable " + v.name->std_str());
    }
    result.get()[i] = m_typeMapper.getTypeOid(it->second);
  }

  return result;

}

std::shared_ptr<QueryResult> Executor::prepareQuery(const StringTemplate& queryTemplate,
                                                    const std::shared_ptr<postgresql::Connection>& connection)
{

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  PGresult *qres = PQprepare(connection->getHandle(),
                             extra->templateName->c_str(),
                             extra->preparedTemplate->c_str(),
                             queryTemplate.getTemplateVariables().size(),
                             extra->paramTypes.get());

  return std::make_shared<QueryResult>(qres, connection, m_connectionProvider, m_resultMapper);

}

std::shared_ptr<QueryResult> Executor::executeQueryPrepared(const StringTemplate& queryTemplate,
                                                            const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                                            const std::shared_ptr<postgresql::Connection>& connection)
{

  QueryParams queryParams(queryTemplate, params, m_typeMapper, m_serializer);

  PGresult *qres = PQexecPrepared(connection->getHandle(),
                                  queryParams.queryName,
                                  queryParams.count,
                                  queryParams.paramValues.data(),
                                  queryParams.paramLengths.data(),
                                  queryParams.paramFormats.data(),
                                  1);

  return std::make_shared<QueryResult>(qres, connection, m_connectionProvider, m_resultMapper);

}

std::shared_ptr<QueryResult> Executor::executeQuery(const StringTemplate& queryTemplate,
                                                    const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                                    const std::shared_ptr<postgresql::Connection>& connection)
{

  QueryParams queryParams(queryTemplate, params, m_typeMapper, m_serializer);

  PGresult *qres = PQexecParams(connection->getHandle(),
                                queryParams.query,
                                queryParams.count,
                                queryParams.paramOids.data(),
                                queryParams.paramValues.data(),
                                queryParams.paramLengths.data(),
                                queryParams.paramFormats.data(),
                                1);

  return std::make_shared<QueryResult>(qres, connection, m_connectionProvider, m_resultMapper);

}

data::share::StringTemplate Executor::parseQueryTemplate(const oatpp::String& name,
                                                         const oatpp::String& text,
                                                         const ParamsTypeMap& paramsTypeMap,
                                                         bool prepare)
{

  auto&& t = ql_template::Parser::parseTemplate(text);

  auto extra = std::make_shared<ql_template::Parser::TemplateExtra>();
  t.setExtraData(extra);

  extra->templateName = name;
  ql_template::TemplateValueProvider valueProvider;
  extra->preparedTemplate = t.format(&valueProvider);

  if(prepare) {
    extra->paramTypes = getParamTypes(t, paramsTypeMap);
  }

  return t;

}

std::shared_ptr<orm::Connection> Executor::getConnection() {
  return m_connectionProvider->get();
}

std::shared_ptr<orm::QueryResult> Executor::execute(const StringTemplate& queryTemplate,
                                                    const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                                    const std::shared_ptr<orm::Connection>& connection,
                                                    bool prepare)
{

  std::shared_ptr<orm::Connection> conn = connection;
  if(!conn) {
    conn = getConnection();
  }

  auto pgConnection = std::static_pointer_cast<postgresql::Connection>(conn);

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  if(prepare) {

    if (!pgConnection->isPrepared(extra->templateName)) {
      auto result = prepareQuery(queryTemplate, pgConnection);
      if(result->isSuccess()) {
        pgConnection->setPrepared(extra->templateName);
      } else {
        return result;
      }
    }

    return executeQueryPrepared(queryTemplate, params, pgConnection);

  }

  return executeQuery(queryTemplate, params, pgConnection);

}

}}
