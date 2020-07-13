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
#include "oatpp/core/data/stream/ChunkedBuffer.hpp"

#include <vector>

namespace oatpp { namespace postgresql {

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

void Executor::prepareQuery(const StringTemplate& queryTemplate,
                            const std::shared_ptr<database::Connection>& connection)
{

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  auto pgConnection = static_cast<PGconn *>(connection->getHandle());
  PGresult *qres = PQprepare(pgConnection,
                             extra->templateName->c_str(),
                             extra->preparedTemplate->c_str(),
                             queryTemplate.getTemplateVariables().size(),
                             extra->paramTypes.get());

  auto status = PQresultStatus(qres);
  if (status != PGRES_COMMAND_OK) {
    OATPP_LOGD("Executor::prepareQuery", "execute prepare failed: %s", PQerrorMessage(pgConnection));
  } else {
    OATPP_LOGD("Executor::prepareQuery", "OK");
  }

}

void Executor::executeQuery(const StringTemplate& queryTemplate,
                            const std::unordered_map<oatpp::String, oatpp::Void>& params,
                            const std::shared_ptr<database::Connection>& connection)
{

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  auto pgConnection = static_cast<PGconn *>(connection->getHandle());

  v_uint32 paramsNumber = queryTemplate.getTemplateVariables().size();

  std::vector<mapping::Serializer::OutputData> outData(paramsNumber);

  std::unique_ptr<const char* []> paramValues(new const char*[paramsNumber]);
  std::unique_ptr<int[]> paramLengths(new int[paramsNumber]);
  std::unique_ptr<int[]> paramFormats(new int[paramsNumber]);

  for(v_uint32 i = 0; i < paramsNumber; i ++) {
    const auto& var = queryTemplate.getTemplateVariables()[i];
    auto it = params.find(var.name);
    if(it == params.end()) {
      throw std::runtime_error("param not found");
    }

    auto& data = outData[i];
    m_serializer.serialize(data, it->second);

    paramValues[i] = data.data;
    paramLengths[i] = data.dataSize;
    paramFormats[i] = data.dataFormat;
  }

  PGresult *qres = PQexecPrepared(pgConnection,
                                  extra->templateName->c_str(),
                                  paramsNumber,
                                  paramValues.get(),
                                  paramLengths.get(),
                                  paramFormats.get(),
                                  0);

  auto status = PQresultStatus(qres);
  if (status != PGRES_TUPLES_OK) {
    OATPP_LOGD("Database", "execute query failed: %s", PQerrorMessage(pgConnection));
  } else {
    OATPP_LOGD("Database", "OK_2");
  }

}

data::share::StringTemplate Executor::parseQueryTemplate(const oatpp::String& name,
                                                         const oatpp::String& text,
                                                         const ParamsTypeMap& paramsTypeMap)
{

  auto&& t = ql_template::Parser::parseTemplate(text);

  auto extra = std::make_shared<ql_template::Parser::TemplateExtra>();
  extra->templateName = name;

  ql_template::TemplateValueProvider valueProvider(&paramsTypeMap);
  extra->preparedTemplate = t.format(&valueProvider);

  extra->paramTypes = getParamTypes(t, paramsTypeMap);

  t.setExtraData(extra);

  return t;

}

std::shared_ptr<database::Connection> Executor::getConnection() {

  oatpp::String dbHost = "localhost";
  oatpp::String dbUser = "postgres";
  oatpp::String dbPassword = "db-pass";
  oatpp::String dbName = "postgres";

  oatpp::data::stream::ChunkedBuffer stream;
  stream << "host=" << dbHost << " user=" << dbUser << " password=" << dbPassword << " dbname=" << dbName;
  auto connStr = stream.toString();

  auto handle = PQconnectdb(connStr->c_str());

  if(PQstatus(handle) == CONNECTION_BAD) {
    OATPP_LOGD("Database", "Connection to database failed: %s\n", PQerrorMessage(handle));
    PQfinish(handle);
    return nullptr;
  }

  return std::make_shared<Connection>(handle);

}

database::QueryResult Executor::execute(const StringTemplate& queryTemplate,
                                        const std::unordered_map<oatpp::String, oatpp::Void>& params,
                                        const std::shared_ptr<database::Connection>& connection)
{

  auto extra = std::static_pointer_cast<ql_template::Parser::TemplateExtra>(queryTemplate.getExtraData());

  std::unordered_map<oatpp::String, oatpp::String> map;
  for(auto p : params) {
    map[p.first] = "<" + p.first + ">";
  }
  auto res = queryTemplate.format(map);

  OATPP_LOGD("AAA", "prepared[%s]={%s}", extra->templateName->c_str(), extra->preparedTemplate->c_str());
  OATPP_LOGD("AAA", "query={%s}", res->c_str());

  prepareQuery(queryTemplate, connection);
  executeQuery(queryTemplate, params, connection);

  return database::QueryResult();
}

}}
