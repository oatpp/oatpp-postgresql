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

  ////

  setSerializerMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Serializer::serializeUuid);
  setSerializerMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Serializer::serializeArray);

}

void Serializer::setTypeOidMethods() {

  m_typeOidMethods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setTypeOidMethod(data::mapping::type::__class::String::CLASS_ID, &Serializer::getTypeOid<TEXTOID>);

  setTypeOidMethod(data::mapping::type::__class::Int8::CLASS_ID, &Serializer::getTypeOid<INT2OID>);
  setTypeOidMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Serializer::getTypeOid<INT2OID>);

  setTypeOidMethod(data::mapping::type::__class::Int16::CLASS_ID, &Serializer::getTypeOid<INT2OID>);
  setTypeOidMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Serializer::getTypeOid<INT4OID>);

  setTypeOidMethod(data::mapping::type::__class::Int32::CLASS_ID, &Serializer::getTypeOid<INT4OID>);
  setTypeOidMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Serializer::getTypeOid<INT8OID>);

  setTypeOidMethod(data::mapping::type::__class::Int64::CLASS_ID, &Serializer::getTypeOid<INT8OID>);

  setTypeOidMethod(data::mapping::type::__class::Float32::CLASS_ID, &Serializer::getTypeOid<FLOAT4OID>);
  setTypeOidMethod(data::mapping::type::__class::Float64::CLASS_ID, &Serializer::getTypeOid<FLOAT8OID>);
  setTypeOidMethod(data::mapping::type::__class::Boolean::CLASS_ID, &Serializer::getTypeOid<BOOLOID>);

  setTypeOidMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Serializer::getTypeOid<FLOAT8ARRAYOID>);

  ////

  setTypeOidMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Serializer::getTypeOid<UUIDOID>);
  setTypeOidMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, &Serializer::getEnumTypeOid);

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
  if(id < m_methods.size()) {
    m_typeOidMethods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Serializer::setTypeOidMethod()]: Error. Unknown classId");
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serializer utility functions

void Serializer::serNull(OutputData& outData) {
  outData.dataBuffer.reset();
  outData.data = nullptr;
  outData.dataSize = 0;
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

void Serializer::serializeArray(const Serializer* _this, OutputData& outData, const oatpp::Void& polymorph) {

    (void) _this;

    if(polymorph) {
        auto v = polymorph.staticCast<oatpp::Vector<Float64>>();

        // get size of header + vector size * sizeof each element (size + data)
        auto dataSize = sizeof(PgArrayHeader) + v->size() * sizeof(PgElem);
        outData.dataBuffer.reset(new char[dataSize]);
        outData.dataSize = dataSize;
        outData.dataFormat = 1;
        outData.oid = FLOAT8ARRAYOID;
        outData.data = outData.dataBuffer.get();
        auto buffer = outData.data;

        // load the data in to the pgArray
        auto *pgArray = reinterpret_cast<PgArray *>(buffer);
        // only support 1d float8 arrays for now
        pgArray->header.ndim = htonl(1);
        pgArray->header._ign = 0;
        pgArray->header.oid = htonl(FLOAT8OID);
        pgArray->header.size = htonl(v->size());
        pgArray->header.index = 0;

        // stuff in the elements in network order
        for (int i=0; i < v->size(); i++) {
            pgArray->elem[i].size = htonl(sizeof(v_float64));
            v_float64 fValue = v->at(i);
            auto pVal = reinterpret_cast<p_int64>(&fValue);
            pgArray->elem[i].value[0] = htonl(*pVal >> 32);
            pgArray->elem[i].value[1] = htonl(*pVal & 0xFFFFFFFF);
        }
    } else{
        serNull(outData);
    }
}

}}}
