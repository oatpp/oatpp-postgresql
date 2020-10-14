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
#include "oatpp/core/data/mapping/TypeResolver.hpp"
#include "oatpp/core/Types.hpp"
#include <libpq-fe.h>

namespace oatpp { namespace postgresql { namespace mapping {

/**
 * Mapper from PostgreSQL result to oatpp objects.
 */
class ResultMapper {
public:

  /**
   * Result data.
   */
  struct ResultData {

    /**
     * Constructor.
     * @param pDbResult
     * @param pTypeResolver
     */
    ResultData(PGresult* pDbResult, const std::shared_ptr<const data::mapping::TypeResolver>& pTypeResolver);

    /**
     * PGResult.
     */
    PGresult* dbResult;

    /**
     * &id:oatpp::data::mapping::TypeResolver;.
     */
    std::shared_ptr<const data::mapping::TypeResolver> typeResolver;

    /**
     * Column names.
     */
    std::vector<oatpp::String> colNames;

    /**
     * Column indices.
     */
    std::unordered_map<data::share::StringKeyLabel, v_int32> colIndices;

    /**
     * Column count.
     */
    v_int64 colCount;

    /**
     * Current row index.
     */
    v_int64 rowIndex;

    /**
     * Row count.
     */
    v_int64 rowCount;

  };

private:
  typedef oatpp::data::mapping::type::Type Type;
  typedef oatpp::Void (*ReadOneRowMethod)(ResultMapper*, ResultData*, const Type*, v_int64);
  typedef oatpp::Void (*ReadRowsMethod)(ResultMapper*, ResultData*, const Type*, v_int64);
private:

  template<class Collection>
  static oatpp::Void readRowAsList(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::PolymorphicDispatcher*>(type->polymorphicDispatcher);
    auto listWrapper = polymorphicDispatcher->createObject();
    const auto& list = listWrapper.template staticCast<Collection>();

    const Type* itemType = *type->params.begin();

    for(v_int32 i = 0; i < dbData->colCount; i ++) {
      mapping::Deserializer::InData inData(dbData->dbResult, rowIndex, i, dbData->typeResolver);
      polymorphicDispatcher->addPolymorphicItem(listWrapper, _this->m_deserializer.deserialize(inData, itemType));
    }

    return listWrapper;

  }

  template<class Collection>
  static oatpp::Void readRowAsKeyValue(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::PolymorphicDispatcher*>(type->polymorphicDispatcher);
    auto mapWrapper = polymorphicDispatcher->createObject();
    const auto& map = mapWrapper.template staticCast<Collection>();

    auto it = type->params.begin();
    const Type* keyType = *it ++;
    if(keyType->classId.id != oatpp::data::mapping::type::__class::String::CLASS_ID.id){
      throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readRowAsKeyValue()]: Invalid map key. Key should be String");
    }

    const Type* valueType = *it;
    for(v_int32 i = 0; i < dbData->colCount; i ++) {
      mapping::Deserializer::InData inData(dbData->dbResult, rowIndex, i, dbData->typeResolver);
      polymorphicDispatcher->addPolymorphicItem(mapWrapper, dbData->colNames[i], _this->m_deserializer.deserialize(inData, valueType));
    }

    return mapWrapper;
  }

  static oatpp::Void readRowAsObject(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex);

  template<class Collection>
  static oatpp::Void readRowsAsList(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 count) {

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::PolymorphicDispatcher*>(type->polymorphicDispatcher);
    auto listWrapper = polymorphicDispatcher->createObject();
    const auto& list = listWrapper.template staticCast<Collection>();

    const Type* itemType = *type->params.begin();

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

  /**
   * Default constructor.
   */
  ResultMapper();

  /**
   * Set "read one row" method for class id.
   * @param classId
   * @param method
   */
  void setReadOneRowMethod(const data::mapping::type::ClassId& classId, ReadOneRowMethod method);

  /**
   * Set "read rows" method for class id.
   * @param classId
   * @param method
   */
  void setReadRowsMethod(const data::mapping::type::ClassId& classId, ReadRowsMethod method);

  /**
   * Read one row to oatpp object or collection. <br>
   * Allowed output type classes are:
   *
   * - &id:oatpp::Vector;
   * - &id:oatpp::List;
   * - &id:oatpp::UnorderedSet;
   * - &id:oatpp::Fields;
   * - &id:oatpp::UnorderedFields;
   * - &id:oatpp::Object;
   *
   * @param dbData
   * @param type
   * @return
   */
  oatpp::Void readOneRow(ResultData* dbData, const Type* type, v_int64 rowIndex);

  /**
   * Read `count` of rows to oatpp collection. <br>
   * Allowed collections to store rows are:
   *
   * - &id:oatpp::Vector;
   * - &id:oatpp::List;
   * - &id:oatpp::UnorderedSet;.
   *
   * @param dbData
   * @param type
   * @param count
   * @return
   */
  oatpp::Void readRows(ResultData* dbData, const Type* type, v_int64 count);

};

}}}

#endif //oatpp_postgresql_mapping_ResultMapper_hpp
