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

#include "Serializer.hpp"

#include "Oid.hpp"
#include "PgArray.hpp"
#include "oatpp-postgresql/Types.hpp"

#if defined(WIN32) || defined(_WIN32)
  #include <WinSock2.h>
#else
  #include <arpa/inet.h>
#endif

namespace oatpp { namespace postgresql { namespace mapping {

Serializer::Serializer() {
  setSerializerMethods();
  setTypeOidMethods();
}

void Serializer::setSerializerMethods() {

  m_methods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setSerializerMethod(data::mapping::type::__class::String::CLASS_ID, &Serializer::serializeString);

  setSerializerMethod(data::mapping::type::__class::Int8::CLASS_ID, &Serializer::serializeInt8);
  setSerializerMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Serializer::serializeUInt8);

  setSerializerMethod(data::mapping::type::__class::Int16::CLASS_ID, &Serializer::serializeInt16);
  setSerializerMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Serializer::serializeUInt16);

  setSerializerMethod(data::mapping::type::__class::Int32::CLASS_ID, &Serializer::serializeInt32);
  setSerializerMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Serializer::serializeUInt32);

  setSerializerMethod(data::mapping::type::__class::Int64::CLASS_ID, &Serializer::serializeInt64);
  setSerializerMethod(data::mapping::type::__class::UInt64::CLASS_ID, &Serializer::serializeUInt64);

  setSerializerMethod(data::mapping::type::__class::Float32::CLASS_ID, &Serializer::serializeFloat32);
  setSerializerMethod(data::mapping::type::__class::Float64::CLASS_ID, &Serializer::serializeFloat64);
  setSerializerMethod(data::mapping::type::__class::Boolean::CLASS_ID, &Serializer::serializeBoolean);

  setSerializerMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, &Serializer::serializeEnum);

  setSerializerMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Serializer::serializeArray<oatpp::AbstractVector>);
  setSerializerMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &Serializer::serializeArray<oatpp::AbstractList>);
  setSerializerMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &Serializer::serializeArray<oatpp::AbstractUnorderedSet>);

  ////

  setSerializerMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Serializer::serializeUuid);

}

void Serializer::setTypeOidMethods() {

  m_typeOidMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);
  m_arrayTypeOidMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setTypeOidMethod(data::mapping::type::__class::String::CLASS_ID, &Serializer::getTypeOid<TEXTOID>);
  setArrayTypeOidMethod(data::mapping::type::__class::String::CLASS_ID, &Serializer::getTypeOid<TEXTARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Int8::CLASS_ID, &Serializer::getTypeOid<INT2OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Int8::CLASS_ID, &Serializer::getTypeOid<INT2ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Serializer::getTypeOid<INT2OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Serializer::getTypeOid<INT2ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Int16::CLASS_ID, &Serializer::getTypeOid<INT2OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Int16::CLASS_ID, &Serializer::getTypeOid<INT2ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Serializer::getTypeOid<INT4OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Serializer::getTypeOid<INT4ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Int32::CLASS_ID, &Serializer::getTypeOid<INT4OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Int32::CLASS_ID, &Serializer::getTypeOid<INT4ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Serializer::getTypeOid<INT8OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Serializer::getTypeOid<INT8ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Int64::CLASS_ID, &Serializer::getTypeOid<INT8OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Int64::CLASS_ID, &Serializer::getTypeOid<INT8ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Float32::CLASS_ID, &Serializer::getTypeOid<FLOAT4OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Float32::CLASS_ID, &Serializer::getTypeOid<FLOAT4ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Float64::CLASS_ID, &Serializer::getTypeOid<FLOAT8OID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Float64::CLASS_ID, &Serializer::getTypeOid<FLOAT8ARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::Boolean::CLASS_ID, &Serializer::getTypeOid<BOOLOID>);
  setArrayTypeOidMethod(data::mapping::type::__class::Boolean::CLASS_ID, &Serializer::getTypeOid<BOOLARRAYOID>);

  setTypeOidMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Serializer::get1DCollectionOid);
  setTypeOidMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &Serializer::get1DCollectionOid);
  setTypeOidMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &Serializer::get1DCollectionOid);

  setTypeOidMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, &Serializer::getEnumTypeOid);
  setArrayTypeOidMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, &Serializer::getEnumArrayTypeOid);

  ////

  setTypeOidMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Serializer::getTypeOid<UUIDOID>);
  setArrayTypeOidMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Serializer::getTypeOid<UUIDARRAYOID>);

}

void Serializer::setSerializerMethod(const data::mapping::type::ClassId& classId, SerializerMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_methods.size()) {
    m_methods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::setSerializerMethod()]: Error. Unknown classId");
  }
}

void Serializer::setTypeOidMethod(const data::mapping::type::ClassId& classId, TypeOidMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_typeOidMethods.size()) {
    m_typeOidMethods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::setTypeOidMethod()]: Error. Unknown classId");
  }
}

void Serializer::setArrayTypeOidMethod(const data::mapping::type::ClassId& classId, TypeOidMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_arrayTypeOidMethods.size()) {
    m_arrayTypeOidMethods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::setArrayTypeOidMethod()]: Error. Unknown classId");
  }
}

void Serializer::serialize(OutputData& outData, const oatpp::Void& polymorph) const {
  auto id = polymorph.valueType->classId.id;
  auto& method = m_methods[id];
  if(method) {
    (*method)(this, outData, polymorph);
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serialize()]: "
                             "Error. No serialize method for type '" + std::string(polymorph.valueType->classId.name) +
                             "'");
  }
}

Oid Serializer::getTypeOid(const oatpp::Type* type) const {

  auto id = type->classId.id;
  auto& method = m_typeOidMethods[id];
  if(method) {
    return (*method)(this, type);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::getTypeOid()]: "
                           "Error. Can't derive OID for type '" + std::string(type->classId.name) +
                           "'");

}

Oid Serializer::getArrayTypeOid(const oatpp::Type* type) const {

  auto id = type->classId.id;
  auto& method = m_arrayTypeOidMethods[id];
  if(method) {
    return (*method)(this, type);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::getArrayTypeOid()]: "
                           "Error. Can't derive OID for type '" + std::string(type->classId.name) +
                           "'");

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serializer utility functions

void Serializer::serNull(OutputData& outData) {
  outData.dataBuffer.reset();
  outData.data = nullptr;
  outData.dataSize = -1;
  outData.dataFormat = 1;
}

void Serializer::serInt2(OutputData& outData, v_int16 value) {
  outData.dataBuffer.reset(new char[2]);
  outData.data = outData.dataBuffer.get();
  outData.dataSize = 2;
  outData.dataFormat = 1;

  *((p_int16) outData.data) = htons(value);
}

void Serializer::serInt4(OutputData& outData, v_int32 value) {
  outData.dataBuffer.reset(new char[4]);
  outData.data = outData.dataBuffer.get();
  outData.dataSize = 4;
  outData.dataFormat = 1;

  *((p_int32) outData.data) = htonl(value);
}

void Serializer::serInt8(OutputData& outData, v_int64 value) {
  outData.dataBuffer.reset(new char[8]);
  outData.data = outData.dataBuffer.get();
  outData.dataSize = 8;
  outData.dataFormat = 1;

  *((p_int32) (outData.data + 0)) = htonl(value >> 32);
  *((p_int32) (outData.data + 4)) = htonl(value & 0xFFFFFFFF);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serializer functions

void Serializer::serializeString(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    base::StrBuffer *buff = static_cast<base::StrBuffer *>(polymorph.get());
    outData.data = (char *)buff->getData();
    outData.dataSize = buff->getSize();
    outData.dataFormat = 1;
    outData.oid = TEXTOID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt8(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Int8>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt8(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::UInt8>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt16(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Int16>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt16(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::UInt16>();
    serInt4(outData, *v);
    outData.oid = INT4OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt32(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Int32>();
    serInt4(outData, *v);
    outData.oid = INT4OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt32(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::UInt32>();
    serInt8(outData, *v);
    outData.oid = INT8OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt64(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Int64>();
    serInt8(outData, *v);
    outData.oid = INT8OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt64(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {
  (void) _this;
  (void) outData;
  (void) polymorph;
  throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeUInt64()]: Error. Not implemented!");
}

void Serializer::serializeFloat32(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Float32>();
    serInt4(outData, *((p_int32) v.get()));
    outData.oid = FLOAT4OID;
  } else{
    serNull(outData);
  }
}

void Serializer::serializeFloat64(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Float64>();
    serInt8(outData, *((p_int64) v.get()));
    outData.oid = FLOAT8OID;
  } else{
    serNull(outData);
  }
}

void Serializer::serializeBoolean(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<oatpp::Boolean>();
    outData.dataBuffer.reset(new char[1]);
    outData.data = outData.dataBuffer.get();
    outData.dataSize = 1;
    outData.dataFormat = 1;
    outData.data[0] = (bool)v;
    outData.oid = BOOLOID;
  } else{
    serNull(outData);
  }
}

void Serializer::serializeEnum(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  auto polymorphicDispatcher = static_cast<const data::mapping::type::__class::AbstractEnum::PolymorphicDispatcher*>(
    polymorph.valueType->polymorphicDispatcher
  );

  data::mapping::type::EnumInterpreterError e = data::mapping::type::EnumInterpreterError::OK;
  const auto& enumInterpretation = polymorphicDispatcher->toInterpretation(polymorph, e);

  if(e == data::mapping::type::EnumInterpreterError::OK) {
    _this->serialize(outData, enumInterpretation);
    return;
  }

  switch(e) {
    case data::mapping::type::EnumInterpreterError::CONSTRAINT_NOT_NULL:
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeEnum()]: Error. Enum constraint violated - 'NotNull'.");
    default:
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeEnum()]: Error. Can't serialize Enum.");
  }

}

Oid Serializer::getEnumTypeOid(const Serializer* _this, const oatpp::Type* type) {

  auto polymorphicDispatcher = static_cast<const data::mapping::type::__class::AbstractEnum::PolymorphicDispatcher*>(
    type->polymorphicDispatcher
  );

  const oatpp::Type* enumInterType = polymorphicDispatcher->getInterpretationType();
  return _this->getTypeOid(enumInterType);

}

Oid Serializer::getEnumArrayTypeOid(const Serializer* _this, const oatpp::Type* type) {

  auto polymorphicDispatcher = static_cast<const data::mapping::type::__class::AbstractEnum::PolymorphicDispatcher*>(
    type->polymorphicDispatcher
  );

  const oatpp::Type* enumInterType = polymorphicDispatcher->getInterpretationType();
  return _this->getArrayTypeOid(enumInterType);

}

Oid Serializer::get1DCollectionOid(const Serializer* _this, const oatpp::Type* type) {

  while(type->classId.id == oatpp::AbstractVector::Class::CLASS_ID.id ||
        type->classId.id == oatpp::AbstractList::Class::CLASS_ID.id ||
        type->classId.id == oatpp::AbstractUnorderedSet::Class::CLASS_ID.id)
  {
    type = *type->params.begin();
  }

  return _this->getArrayTypeOid(type);

}

void Serializer::serializeUuid(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.staticCast<postgresql::Uuid>();
    outData.data = (char*) v->getData();
    outData.dataSize = v->getSize();
    outData.dataFormat = 1;
    outData.oid = UUIDOID;
  } else{
    serNull(outData);
  }
}

const oatpp::Type* Serializer::getArrayItemTypeAndDimensions(const oatpp::Void& polymorph, std::vector<v_int32>& dimensions) {

  void* currObj = polymorph.get();
  const oatpp::Type* currType = polymorph.valueType;

  while(currType->classId.id == oatpp::AbstractVector::Class::CLASS_ID.id ||
        currType->classId.id == oatpp::AbstractList::Class::CLASS_ID.id ||
        currType->classId.id == oatpp::AbstractUnorderedSet::Class::CLASS_ID.id)
  {

    if(currObj == nullptr) {
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::getArrayItemTypeAndDimensions()]: Error. "
                               "The nested container can't be null.");
    }

    if(currType->classId.id == oatpp::AbstractVector::Class::CLASS_ID.id) {

      auto c = static_cast<std::vector<oatpp::Void>*>(currObj);
      dimensions.push_back(c->size());
      currObj = (c->size() > 0) ? (*c)[0].get() : nullptr;

    } else if(currType->classId.id == oatpp::AbstractList::Class::CLASS_ID.id) {

      auto c = static_cast<std::list<oatpp::Void>*>(currObj);
      dimensions.push_back(c->size());
      currObj = (c->size() > 0) ? c->front().get() : nullptr;


    } else if(currType->classId.id == oatpp::AbstractUnorderedSet::Class::CLASS_ID.id) {

      auto c = static_cast<std::unordered_set<oatpp::Void>*>(currObj);
      dimensions.push_back(c->size());
      currObj = (c->size() > 0) ? c->begin()->get() : nullptr;

    }

    currType = *currType->params.begin();

  }

  return currType;

}

void Serializer::writeArrayHeader(data::stream::ConsistentOutputStream* stream,
                                  Oid itemOid,
                                  const std::vector<v_int32>& dimensions)
{

  // num dimensions
  v_int32 v = htonl(dimensions.size());
  stream->writeSimple(&v, sizeof(v_int32));

  // ignore
  v = 0;
  stream->writeSimple(&v, sizeof(v_int32));

  // oid
  v = htonl(itemOid);
  stream->writeSimple(&v, sizeof(v_int32));

  // size
  v = htonl(dimensions[0]);
  stream->writeSimple(&v, sizeof(v_int32));

  // index
  v = htonl(1);
  stream->writeSimple(&v, sizeof(v_int32));

  for(v_uint32 i = 1; i < dimensions.size(); i++) {
    v_int32 size = htonl(dimensions[i]);
    v_int32 index = htonl(1);
    stream->writeSimple(&size, sizeof(v_int32));
    stream->writeSimple(&index, sizeof(v_int32));
  }

}

void Serializer::serializeSubArray(data::stream::ConsistentOutputStream* stream,
                                   const oatpp::Void& polymorph,
                                   ArraySerializationMeta& meta,
                                   v_int32 dimension)
{

  const oatpp::Type* type = polymorph.valueType;

  if(data::mapping::type::__class::AbstractVector::CLASS_ID.id == type->classId.id) {
    return serializeSubArray<oatpp::AbstractVector>(stream, polymorph, meta, dimension);

  } else if(data::mapping::type::__class::AbstractList::CLASS_ID.id == type->classId.id) {
    return serializeSubArray<oatpp::AbstractList>(stream, polymorph, meta, dimension);

  } else if(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID.id == type->classId.id) {
    return serializeSubArray<oatpp::AbstractUnorderedSet>(stream, polymorph, meta, dimension);

  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeSubArray()]: "
                           "Error. Unknown 1D collection type.");

}

}}}
