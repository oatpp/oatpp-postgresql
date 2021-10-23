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
                         const provider::ResourceHandle<orm::Connection>& connection,
                         const std::shared_ptr<mapping::ResultMapper>& resultMapper,
                         const std::shared_ptr<const data::mapping::TypeResolver>& typeResolver)
  : m_dbResult(dbResult)
  , m_connection(connection)
  , m_resultMapper(resultMapper)
  , m_resultData(dbResult, typeResolver)
{
  auto status = PQresultStatus(m_dbResult);
  switch(status) {

    case PGRES_SINGLE_TUPLE: {
      throw std::runtime_error("[oatpp::postgresql::QueryResult::QueryResult()]: Error. Single-row mode is not supported!");
    }

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
      if(status == PGRES_FATAL_ERROR) {
        connection.invalidator->invalidate(connection.object);
      }
    }

  }
}

QueryResult::~QueryResult() {
  PQclear(m_dbResult);
}

provider::ResourceHandle<orm::Connection> QueryResult::getConnection() const {
  return provider::ResourceHandle<orm::Connection>(m_connection.object, m_connection.invalidator);
}

bool QueryResult::isSuccess() const {
  return m_success;
}

oatpp::String QueryResult::getErrorMessage() const {
  if(!m_success) {
    auto pgConnection = std::static_pointer_cast<postgresql::Connection>(m_connection.object);
    return PQerrorMessage(pgConnection->getHandle());
  }
  return nullptr;
}

v_int64 QueryResult::getPosition() const {
  return m_resultData.rowIndex;
}

v_int64 QueryResult::getKnownCount() const {
  switch(m_type) {
    case TYPE_TUPLES: return m_resultData.rowCount;
//    case TYPE_COMMAND: return 0;
  }
  return 0;
}

bool QueryResult::hasMoreToFetch() const {
  return getKnownCount() > 0;
}

oatpp::Void QueryResult::fetch(const oatpp::Type* const resultType, v_int64 count) {
  return m_resultMapper->readRows(&m_resultData, resultType, count);
}

}}
