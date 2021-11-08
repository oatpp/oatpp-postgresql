/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Don Smyth <don.smyth@gmail.com>
 *                         Leonid Stryzhevskyi <lganzzzo@gmail.com>
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

  setSerializerMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Serializer::serializeArray);
  setSerializerMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &Serializer::serializeArray);
  setSerializerMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &Serializer::serializeArray);

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
  if(id >= m_methods.size()) {
    m_methods.resize(id + 1, nullptr);
  }
  m_methods[id] = method;
}

void Serializer::setTypeOidMethod(const data::mapping::type::ClassId& classId, TypeOidMethod method) {
  const v_uint32 id = classId.id;
  if(id >= m_typeOidMethods.size()) {
    m_typeOidMethods.resize(id + 1, nullptr);
  }
  m_typeOidMethods[id] = method;
}

void Serializer::setArrayTypeOidMethod(const data::mapping::type::ClassId& classId, TypeOidMethod method) {
  const v_uint32 id = classId.id;
  if(id >= m_arrayTypeOidMethods.size()) {
    m_arrayTypeOidMethods.resize(id + 1, nullptr);
  }
  m_arrayTypeOidMethods[id] = method;
}

void Serializer::serialize(OutputData& outData, const oatpp::Void& polymorph) const {
  auto id = polymorph.getValueType()->classId.id;
  auto& method = m_methods[id];
  if(method) {
    (*method)(this, outData, polymorph);
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serialize()]: "
                             "Error. No serialize method for type '" + std::string(polymorph.getValueType()->classId.name) +
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
    std::string* buff = static_cast<std::string*>(polymorph.get());
    outData.data = (char *)buff->data();
    outData.dataSize = buff->size();
    outData.dataFormat = 1;
    outData.oid = TEXTOID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt8(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Int8>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt8(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::UInt8>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt16(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Int16>();
    serInt2(outData, *v);
    outData.oid = INT2OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt16(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::UInt16>();
    serInt4(outData, *v);
    outData.oid = INT4OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt32(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Int32>();
    serInt4(outData, *v);
    outData.oid = INT4OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeUInt32(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::UInt32>();
    serInt8(outData, *v);
    outData.oid = INT8OID;
  } else {
    serNull(outData);
  }
}

void Serializer::serializeInt64(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Int64>();
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
    auto v = polymorph.cast<oatpp::Float32>();
    serInt4(outData, *((p_int32) v.get()));
    outData.oid = FLOAT4OID;
  } else{
    serNull(outData);
  }
}

void Serializer::serializeFloat64(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Float64>();
    serInt8(outData, *((p_int64) v.get()));
    outData.oid = FLOAT8OID;
  } else{
    serNull(outData);
  }
}

void Serializer::serializeBoolean(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  (void) _this;

  if(polymorph) {
    auto v = polymorph.cast<oatpp::Boolean>();
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
    polymorph.getValueType()->polymorphicDispatcher
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
    auto v = polymorph.cast<postgresql::Uuid>();
    outData.data = (char*) v->getData();
    outData.dataSize = v->getSize();
    outData.dataFormat = 1;
    outData.oid = UUIDOID;
  } else{
    serNull(outData);
  }
}

const oatpp::Type* Serializer::getArrayItemTypeAndDimensions(const oatpp::Void& polymorph, std::vector<v_int32>& dimensions) {

  oatpp::Void curr = polymorph;

  while(curr.getValueType()->isCollection) {

    if(curr == nullptr) {
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::getArrayItemTypeAndDimensions()]: Error. "
                               "The nested container can't be null.");
    }

    auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(curr.getValueType()->polymorphicDispatcher);
    auto size = dispatcher->getCollectionSize(curr);
    dimensions.push_back(size);

    if(size > 0) {
      auto iterator = dispatcher->beginIteration(curr);
      curr = iterator->get();
    } else {
      curr = nullptr;
    }

  }

  return curr.getValueType();

}

void Serializer::serializeSubArray(data::stream::ConsistentOutputStream* stream,
                                   const oatpp::Void& polymorph,
                                   ArraySerializationMeta& meta,
                                   v_int32 dimension)
{

  const oatpp::Type* type = polymorph.getValueType();
  if(!type->isCollection) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeSubArray()]: Error. Unknown collection type.");
  }

  auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  const oatpp::Type* itemType = dispatcher->getItemType();

  if(dimension < meta.dimensions.size() - 1) {

    auto size = meta.dimensions[dimension];

    if(dispatcher->getCollectionSize(polymorph) != size) {
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeSubArray()]. Error. "
                               "All nested arrays must be of the same size.");
    }

    auto iterator = dispatcher->beginIteration(polymorph);
    while (!iterator->finished()) {
      serializeSubArray(stream, iterator->get(), meta, dimension + 1);
      iterator->next();
    }

  } else if(dimension == meta.dimensions.size() - 1) {

    auto size = meta.dimensions[dimension];

    if(dispatcher->getCollectionSize(polymorph) != size) {
      throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeSubArray()]. Error. "
                               "All nested arrays must be of the same size.");
    }

    auto iterator = dispatcher->beginIteration(polymorph);
    while (!iterator->finished()) {

      OutputData data;
      meta._this->serialize(data, iterator->get());

      v_int32 itemSize = htonl(data.dataSize);
      stream->writeSimple(&itemSize, sizeof(v_int32));

      if(data.data != nullptr) {
        stream->writeSimple(data.data, data.dataSize);
      }

      iterator->next();
    }

  }

}

void Serializer::serializeArray(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

  if(!polymorph) {
    serNull(outData);
  }

  ArraySerializationMeta meta;
  meta._this = _this;
  const oatpp::Type* itemType = getArrayItemTypeAndDimensions(polymorph, meta.dimensions);

  if(meta.dimensions.empty()) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::serializeArray()]: Error. "
                             "Invalid array.");
  }

  data::stream::BufferOutputStream stream;
  ArrayUtils::writeArrayHeader(&stream, _this->getTypeOid(itemType), meta.dimensions);

  serializeSubArray(&stream, polymorph, meta, 0);

  outData.oid = _this->getArrayTypeOid(itemType);
  outData.dataSize = stream.getCurrentPosition();
  outData.dataBuffer.reset(new char[outData.dataSize]);
  outData.data = outData.dataBuffer.get();
  outData.dataFormat = 1;

  std::memcpy(outData.data, stream.getData(), outData.dataSize);

}

}}}
