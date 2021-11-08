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

ResultMapper::ResultData::ResultData(PGresult* pDbResult, const std::shared_ptr<const data::mapping::TypeResolver>& pTypeResolver)
  : dbResult(pDbResult)
  , typeResolver(pTypeResolver)
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

    setReadOneRowMethod(data::mapping::type::__class::AbstractObject::CLASS_ID, &ResultMapper::readOneRowAsObject);

    setReadOneRowMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &ResultMapper::readOneRowAsCollection);
    setReadOneRowMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &ResultMapper::readOneRowAsCollection);
    setReadOneRowMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &ResultMapper::readOneRowAsCollection);

    setReadOneRowMethod(data::mapping::type::__class::AbstractPairList::CLASS_ID, &ResultMapper::readOneRowAsMap);
    setReadOneRowMethod(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, &ResultMapper::readOneRowAsMap);
  }

  {
    m_readRowsMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

    setReadRowsMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &ResultMapper::readRowsAsCollection);
    setReadRowsMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &ResultMapper::readRowsAsCollection);
    setReadRowsMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &ResultMapper::readRowsAsCollection);

  }

}

void ResultMapper::setReadOneRowMethod(const data::mapping::type::ClassId& classId, ReadOneRowMethod method) {
  const v_uint32 id = classId.id;
  if(id >= m_readOneRowMethods.size()) {
    m_readOneRowMethods.resize(id + 1, nullptr);
  }
  m_readOneRowMethods[id] = method;
}

void ResultMapper::setReadRowsMethod(const data::mapping::type::ClassId& classId, ReadRowsMethod method) {
  const v_uint32 id = classId.id;
  if(id >= m_readRowsMethods.size()) {
    m_readRowsMethods.resize(id + 1, nullptr);
  }
  m_readRowsMethods[id] = method;
}

oatpp::Void ResultMapper::readOneRowAsCollection(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

  auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  auto collection = dispatcher->createObject();

  const Type* itemType = *type->params.begin();

  for(v_int32 i = 0; i < dbData->colCount; i ++) {
    mapping::Deserializer::InData inData(dbData->dbResult, rowIndex, i, dbData->typeResolver);
    dispatcher->addItem(collection, _this->m_deserializer.deserialize(inData, itemType));
  }

  return collection;

}

oatpp::Void ResultMapper::readOneRowAsMap(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

  auto dispatcher = static_cast<const data::mapping::type::__class::Map::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  auto map = dispatcher->createObject();

  const Type* keyType = dispatcher->getKeyType();
  if(keyType->classId.id != oatpp::data::mapping::type::__class::String::CLASS_ID.id){
    throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readOneRowAsMap()]: Invalid map key. Key should be String");
  }

  const Type* valueType = map.getValueType();
  for(v_int32 i = 0; i < dbData->colCount; i ++) {
    mapping::Deserializer::InData inData(dbData->dbResult, rowIndex, i, dbData->typeResolver);
    dispatcher->addItem(map, dbData->colNames[i], _this->m_deserializer.deserialize(inData, valueType));
  }

  return map;

}

oatpp::Void ResultMapper::readOneRowAsObject(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 rowIndex) {

  auto dispatcher = static_cast<const data::mapping::type::__class::AbstractObject::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  auto object = dispatcher->createObject();
  const auto& fieldsMap = dispatcher->getProperties()->getMap();

  for(v_int32 i = 0; i < dbData->colCount; i ++) {

    auto it = fieldsMap.find(*dbData->colNames[i]);

    if(it != fieldsMap.end()) {
      auto field = it->second;
      mapping::Deserializer::InData inData(dbData->dbResult, rowIndex, i, dbData->typeResolver);
      field->set(static_cast<oatpp::BaseObject*>(object.get()), _this->m_deserializer.deserialize(inData, field->type));
    } else {
      OATPP_LOGE("[oatpp::postgresql::mapping::ResultMapper::readRowAsObject]",
                 "Error. The object of type '%s' has no field to map column '%s'.",
                 type->nameQualifier, dbData->colNames[i]->c_str());
      throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readRowAsObject]: Error. "
                               "The object of type " + std::string(type->nameQualifier) +
                               " has no field to map column " + *dbData->colNames[i] + ".");
    }

  }

  return object;

}

oatpp::Void ResultMapper::readRowsAsCollection(ResultMapper* _this, ResultData* dbData, const Type* type, v_int64 count) {

  auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  auto collection = dispatcher->createObject();

  const Type* itemType = dispatcher->getItemType();

  auto leftCount = dbData->rowCount - dbData->rowIndex;
  auto wantToRead = count;
  if(wantToRead > leftCount) {
    wantToRead = leftCount;
  }

  for(v_int64 i = 0; i < wantToRead; i++) {
    dispatcher->addItem(collection, _this->readOneRow(dbData, itemType, dbData->rowIndex));
    ++ dbData->rowIndex;
  }

  return collection;

}

oatpp::Void ResultMapper::readOneRow(ResultData* dbData, const Type* type, v_int64 rowIndex) {

  auto id = type->classId.id;
  auto& method = m_readOneRowMethods[id];

  if(method) {
    return (*method)(this, dbData, type, rowIndex);
  }

  auto* interpretation = type->findInterpretation(dbData->typeResolver->getEnabledInterpretations());
  if(interpretation) {
    return interpretation->fromInterpretation(readOneRow(dbData, interpretation->getInterpretationType(), rowIndex));
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::ResultMapper::readOneRow()]: "
                           "Error. Invalid result container type. "
                           "Allowed types are "
                           "oatpp::Vector, "
                           "oatpp::List, "
                           "oatpp::UnorderedSet, "
                           "oatpp::Fields, "
                           "oatpp::UnorderedFields, "
                           "oatpp::Object");

}

oatpp::Void ResultMapper::readRows(ResultData* dbData, const Type* type, v_int64 count) {

  if(count == -1) {
    count = dbData->rowCount;
  }

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
