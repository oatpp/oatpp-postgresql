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

#include "QueryResult.hpp"

namespace oatpp { namespace postgresql {

QueryResult::QueryResult(PGresult* dbResult,
                         const std::shared_ptr<Connection>& connection,
                         const std::shared_ptr<mapping::ResultMapper>& resultMapper)
  : m_dbResult(dbResult)
  , m_connection(connection)
  , m_resultMapper(resultMapper)
  , m_resultData(dbResult)
{
  auto status = PQresultStatus(m_dbResult);
  switch(status) {
    case PGRES_TUPLES_OK: {
      m_success = true;
      m_type = TYPE_TUPLES;
      break;
    }

    case PGRES_COMMAND_OK: {
      m_success = true;
      m_type = TYPE_COMMAND;
      break;
    }

    default: {
      m_success = false;
      m_type = TYPE_ERROR;
    }
  }
}

QueryResult::~QueryResult() {
  PQclear(m_dbResult);
}

bool QueryResult::isSuccess() {
  return m_success;
}

v_int64 QueryResult::count() {
  switch(m_type) {
    case TYPE_TUPLES: return PQntuples(m_dbResult);
//    case TYPE_COMMAND: return 0;
  }
  return 0;
}

std::vector<std::vector<oatpp::Void>> QueryResult::fetchRows(v_int64 count) {
  return {};
}

void QueryResult::fetch(oatpp::Void& polymorph, v_int64 count) {
  polymorph = m_resultMapper->readRows(&m_resultData, polymorph.valueType, count);
}

}}
