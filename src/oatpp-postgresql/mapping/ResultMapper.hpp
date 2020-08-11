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

#ifndef oatpp_postgresql_mapping_ResultMapper_hpp
#define oatpp_postgresql_mapping_ResultMapper_hpp

#include "Deserializer.hpp"
#include "oatpp/core/Types.hpp"
#include <libpq-fe.h>

namespace oatpp { namespace postgresql { namespace mapping {

class ResultMapper {
public:

  struct ResultData {

    ResultData(PGresult* pDbResult);

    PGresult* dbResult;
    std::vector<oatpp::String> colNames;
    std::unordered_map<data::share::StringKeyLabel, v_int32> colIndices;
    v_int64 colCount;
    v_int64 rowIndex;
    v_int64 rowCount;

  };

private:
  typedef oatpp::data::mapping::type::Type Type;
  typedef oatpp::Void (*ReadOneRowMethod)(ResultMapper*, ResultData*, const Type*, v_int64);
  typedef oatpp::Void (*ReadRowsMethod)(ResultMapper*, ResultData*, const Type*, v_int64);
private:

  template<class Collection>
  static oatpp::Void readRowAsList(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

    auto listWrapper = type->creator();

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::AbstractPolymorphicDispatcher*>(type->polymorphicDispatcher);
    const auto& list = listWrapper.template staticCast<Collection>();

    Type* itemType = *type->params.begin();

    for(v_int32 i = 0; i < dbData->colCount; i ++) {

      mapping::Deserializer::InData inData;

      inData.oid = PQftype(dbData->dbResult, i);
      inData.size = PQfsize(dbData->dbResult, i);
      inData.data = PQgetvalue(dbData->dbResult, rowIndex, i);

      polymorphicDispatcher->addPolymorphicItem(listWrapper, _this->m_deserializer.deserialize(inData, itemType));

    }

    return listWrapper;

  }

  template<class Collection>
  static oatpp::Void readRowAsKeyValue(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

    auto mapWrapper = type->creator();
    auto polymorphicDispatcher = static_cast<const typename Collection::Class::AbstractPolymorphicDispatcher*>(type->polymorphicDispatcher);
    const auto& map = mapWrapper.template staticCast<Collection>();

    auto it = type->params.begin();
    Type* keyType = *it ++;
    if(keyType->classId.id != oatpp::data::mapping::type::__class::String::CLASS_ID.id){
      throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readRowAsKeyValue()]: Invalid map key. Key should be String");
    }
    Type* valueType = *it;

    for(v_int32 i = 0; i < dbData->colCount; i ++) {

      mapping::Deserializer::InData inData;

      inData.oid = PQftype(dbData->dbResult, i);
      inData.size = PQfsize(dbData->dbResult, i);
      inData.data = PQgetvalue(dbData->dbResult, rowIndex, i);

      polymorphicDispatcher->addPolymorphicItem(mapWrapper, dbData->colNames[i], _this->m_deserializer.deserialize(inData, valueType));

    }

    return mapWrapper;
  }

  static oatpp::Void readRowAsObject(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex);

  template<class Collection>
  static oatpp::Void readRowsAsList(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 count) {

    auto listWrapper = type->creator();

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::AbstractPolymorphicDispatcher*>(type->polymorphicDispatcher);
    const auto& list = listWrapper.template staticCast<Collection>();

    Type* itemType = *type->params.begin();

    auto leftCount = dbData->rowCount - dbData->rowIndex;
    auto wantToRead = count;
    if(wantToRead > leftCount) {
      wantToRead = leftCount;
    }

    for(v_int64 i = 0; i < wantToRead; i++) {
      polymorphicDispatcher->addPolymorphicItem(listWrapper, _this->readOneRow(dbData, itemType, dbData->rowIndex));
      ++ dbData->rowIndex;
    }

    return listWrapper;

  }

private:
  Deserializer m_deserializer;
  std::vector<ReadOneRowMethod> m_readOneRowMethods;
  std::vector<ReadRowsMethod> m_readRowsMethods;
public:

  ResultMapper();

  void setReadOneRowMethod(const data::mapping::type::ClassId& classId, ReadOneRowMethod method);
  void setReadRowsMethod(const data::mapping::type::ClassId& classId, ReadRowsMethod method);

  oatpp::Void readOneRow(ResultData* dbData, const Type* type, v_int64 rowIndex);
  oatpp::Void readRows(ResultData* dbData, const Type* type, v_int64 count);

};

}}}

#endif //oatpp_postgresql_mapping_ResultMapper_hpp
