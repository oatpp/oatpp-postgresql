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

QueryResult::QueryResult(PGresult* dbResult, const std::shared_ptr<Connection>& connection)
  : m_dbResult(dbResult)
  , m_connection(connection)
  , m_cursor(0)
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

  std::vector<std::vector<oatpp::Void>> result;

  auto leftCount = this->count() - m_cursor;
  auto wantToRead = count;
  if(wantToRead > leftCount) {
    wantToRead = leftCount;
  }

  auto fieldsCount = PQnfields(m_dbResult);

  for(v_int64 i = 0; i < wantToRead; i++) {

    std::vector<oatpp::Void> row(fieldsCount);

    for(v_int32 fieldIndex = 0; fieldIndex < fieldsCount; fieldIndex ++) {
      auto oid = PQftype(m_dbResult, fieldIndex);
      auto size = PQfsize(m_dbResult, fieldIndex);
      char* data = PQgetvalue(m_dbResult, m_cursor, fieldIndex);
      // TODO map
    }

    result.push_back(std::move(row));

    ++ m_cursor;

  }

  return result;

}

void QueryResult::fetch(oatpp::Void& polymorph, v_int64 count) {

  auto type = polymorph.valueType;

  if(type->classId.id == oatpp::data::mapping::type::__class::AbstractVector::CLASS_ID.id) {
    fetchAsList<oatpp::AbstractVector>(polymorph, count);
  } else if(type->classId.id == oatpp::data::mapping::type::__class::AbstractList::CLASS_ID.id) {
    fetchAsList<oatpp::AbstractList>(polymorph, count);
  } else if(type->classId.id == oatpp::data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID.id) {
    fetchAsList<oatpp::AbstractUnorderedSet>(polymorph, count);
  } else {
    throw std::runtime_error("[oatpp::postgresql::QueryResult::fetch()]: "
                             "Error. Invalid result container type. "
                             "Allowed types are oatpp::Vector, oatpp::List, oatpp::UnorderedSet");
  }

}

oatpp::Void QueryResult::readRow(Type* type, v_int64 rowIndex) {
  if(type->classId.id == oatpp::data::mapping::type::__class::AbstractVector::CLASS_ID.id) {
    return readRowAsList<oatpp::AbstractVector>(this, type, rowIndex);
  } else if(type->classId.id == oatpp::data::mapping::type::__class::AbstractList::CLASS_ID.id) {
    return readRowAsList<oatpp::AbstractList>(this, type, rowIndex);
  } else if(type->classId.id == oatpp::data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID.id) {
    return readRowAsList<oatpp::AbstractUnorderedSet>(this, type, rowIndex);
  } else {
    throw std::runtime_error("[oatpp::postgresql::QueryResult::readRow()]: "
                             "Error. Invalid result container type. "
                             "Allowed types are oatpp::Vector, oatpp::List, oatpp::UnorderedSet");
  }
}

}}
