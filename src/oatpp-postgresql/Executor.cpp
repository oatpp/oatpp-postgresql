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

namespace oatpp { namespace postgresql {

data::share::StringTemplate Executor::parseQueryTemplate(const oatpp::String& name,
                                                         const oatpp::String& text,
                                                         const ParamsTypeMap& paramsTypeMap) {

  auto&& t = ql_template::Parser::parseTemplate(text);

  auto extra = std::make_shared<ql_template::Parser::TemplateExtra>();
  extra->templateName = name;

  ql_template::TemplateValueProvider valueProvider(&paramsTypeMap);
  extra->preparedTemplate = t.format(&valueProvider);

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

  {
    auto pgConnection = static_cast<PGconn *>(connection->getHandle());
    PGresult *qres = PQprepare(pgConnection,
                               extra->templateName->c_str(),
                               extra->preparedTemplate->c_str(),
                               queryTemplate.getTemplateVariables().size(),
                               nullptr);

    auto status = PQresultStatus(qres);
    if (status != PGRES_COMMAND_OK) {
      OATPP_LOGD("Database", "execute prepare failed: %s", PQerrorMessage(pgConnection));
    } else {
      OATPP_LOGD("Database", "OK_1");
    }

  }

  {
    auto pgConnection = static_cast<PGconn *>(connection->getHandle());

    v_int32 paramsNumber = queryTemplate.getTemplateVariables().size();
    std::unique_ptr<char* []> params(new char*[paramsNumber]);

    params[0] = "test3";
    params[1] = "test3";
    params[2] = "test3";

    PGresult *qres = PQexecPrepared(pgConnection,
                                    extra->templateName->c_str(),
                                    paramsNumber,
                                    params.get(),
                                    nullptr,
                                    nullptr,
                                    0);

    auto status = PQresultStatus(qres);
    if (status != PGRES_TUPLES_OK) {
      OATPP_LOGD("Database", "execute query failed: %s", PQerrorMessage(pgConnection));
    } else {
      OATPP_LOGD("Database", "OK_2");
    }

  }

  return database::QueryResult();
}

}}
