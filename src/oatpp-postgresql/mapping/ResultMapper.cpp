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

#include "ResultMapper.hpp"

namespace oatpp { namespace postgresql { namespace mapping {

ResultMapper::ResultData::ResultData(PGresult* pDbResult)
  : dbResult(pDbResult)
{

  rowIndex = 0;
  rowCount = PQntuples(dbResult);

  {
    colCount = PQnfields(dbResult);
    for (v_int32 i = 0; i < colCount; i++) {
      oatpp::String colName = (const char*) PQfname(dbResult, i);
      colNames.push_back(colName);
      colIndices.insert({colName, i});
    }
  }

}

ResultMapper::ResultMapper() {

  {
    m_readOneRowMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

    setReadOneRowMethod(data::mapping::type::__class::AbstractObject::CLASS_ID, nullptr);

    setReadOneRowMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &ResultMapper::readRowAsList<oatpp::AbstractVector>);
    setReadOneRowMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &ResultMapper::readRowAsList<oatpp::AbstractList>);
    setReadOneRowMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &ResultMapper::readRowAsList<oatpp::AbstractUnorderedSet>);

    setReadOneRowMethod(data::mapping::type::__class::AbstractPairList::CLASS_ID, nullptr);
    setReadOneRowMethod(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, nullptr);
  }

  {
    m_readRowsMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

    setReadRowsMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &ResultMapper::readRowsAsList<oatpp::AbstractVector>);
    setReadRowsMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &ResultMapper::readRowsAsList<oatpp::AbstractList>);
    setReadRowsMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &ResultMapper::readRowsAsList<oatpp::AbstractUnorderedSet>);

  }

}

void ResultMapper::setReadOneRowMethod(const data::mapping::type::ClassId& classId, ReadOneRowMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_readOneRowMethods.size()) {
    m_readOneRowMethods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::setReadOneRowMethod()]: Error. Unknown classId");
  }
}

void ResultMapper::setReadRowsMethod(const data::mapping::type::ClassId& classId, ReadRowsMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_readRowsMethods.size()) {
    m_readRowsMethods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::setReadRowsMethod()]: Error. Unknown classId");
  }
}

oatpp::Void ResultMapper::readOneRow(ResultData* dbData, const Type* type, v_int64 rowIndex) {

  auto id = type->classId.id;
  auto& method = m_readOneRowMethods[id];

  if(method) {
    return (*method)(this, dbData, type, rowIndex);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readOneRow()]: "
                           "Error. Invalid result container type. "
                           "Allowed types are oatpp::Vector, oatpp::List, oatpp::UnorderedSet");

}

oatpp::Void ResultMapper::readRows(ResultData* dbData, const Type* type, v_int64 count) {

  auto id = type->classId.id;
  auto& method = m_readRowsMethods[id];

  if(method) {
    return (*method)(this, dbData, type, count);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readRows()]: "
                           "Error. Invalid result container type. "
                           "Allowed types are oatpp::Vector, oatpp::List, oatpp::UnorderedSet");

}

}}}
