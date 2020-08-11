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

#ifndef oatpp_postgresql_QueryResult_hpp
#define oatpp_postgresql_QueryResult_hpp

#include "Connection.hpp"
#include "mapping/Deserializer.hpp"
#include "oatpp/orm/QueryResult.hpp"

namespace oatpp { namespace postgresql {

class QueryResult : public orm::QueryResult {
private:
  static constexpr v_int32 TYPE_ERROR = 0;
  static constexpr v_int32 TYPE_COMMAND = 1;
  static constexpr v_int32 TYPE_TUPLES = 2;
private:
  typedef oatpp::data::mapping::type::Type Type;
private:

  template<class Collection>
  static oatpp::Void readRowAsList(QueryResult* _this, Type* type, v_int64 rowIndex) {

    auto listWrapper = type->creator();

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::AbstractPolymorphicDispatcher*>(type->polymorphicDispatcher);
    const auto& list = listWrapper.template staticCast<Collection>();

    Type* itemType = *type->params.begin();

    auto fieldsCount = PQnfields(_this->m_dbResult);

    for(v_int32 fieldIndex = 0; fieldIndex < fieldsCount; fieldIndex ++) {

      mapping::Deserializer::InData inData;

      inData.oid = PQftype(_this->m_dbResult, fieldIndex);
      inData.size = PQfsize(_this->m_dbResult, fieldIndex);
      inData.data = PQgetvalue(_this->m_dbResult, rowIndex, fieldIndex);

      polymorphicDispatcher->addPolymorphicItem(listWrapper, _this->m_deserializer.deserialize(inData, itemType));

    }

    return listWrapper;

  }

  template<class Collection>
  static oatpp::Void readRowAsKeyValue(QueryResult* _this, Type* type, v_int64 rowIndex) {
    return nullptr;
  }

  oatpp::Void readRow(Type* type, v_int64 rowIndex);

private:

  template<class Collection>
  void fetchAsList(oatpp::Void& polymorph, v_int64 count) {

    auto type = polymorph.valueType;

    auto listWrapper = type->creator();
    polymorph = listWrapper;

    auto polymorphicDispatcher = static_cast<const typename Collection::Class::AbstractPolymorphicDispatcher*>(type->polymorphicDispatcher);
    const auto& list = listWrapper.template staticCast<Collection>();

    Type* itemType = *type->params.begin();

    auto leftCount = this->count() - m_cursor;
    auto wantToRead = count;
    if(wantToRead > leftCount) {
      wantToRead = leftCount;
    }

    for(v_int64 i = 0; i < wantToRead; i++) {
      polymorphicDispatcher->addPolymorphicItem(listWrapper, readRow(itemType, m_cursor));
      ++ m_cursor;
    }

  }

private:
  PGresult* m_dbResult;
  std::shared_ptr<Connection> m_connection;
  v_int64 m_cursor;
  bool m_success;
  v_int32 m_type;
private:
  mapping::Deserializer m_deserializer;
public:

  QueryResult(PGresult* dbResult, const std::shared_ptr<Connection>& connection);

  ~QueryResult();

  bool isSuccess() override;

  v_int64 count() override;

  std::vector<std::vector<oatpp::Void>> fetchRows(v_int64 count) override;

  void fetch(oatpp::Void& polymorph, v_int64 count) override;

};

}}

#endif //oatpp_postgresql_QueryResult_hpp
