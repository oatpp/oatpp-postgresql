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

#include "Deserializer.hpp"

#include "Oid.hpp"

#if defined(WIN32) || defined(_WIN32)
  #include <WinSock2.h>
#else
  #include <arpa/inet.h>
#endif

namespace oatpp { namespace postgresql { namespace mapping {

Deserializer::Deserializer() {

  m_methods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setDeserializerMethod(data::mapping::type::__class::String::CLASS_ID, &Deserializer::deserializeString);
  setDeserializerMethod(data::mapping::type::__class::Any::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::Int8::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int8>);
  setDeserializerMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt8>);

  setDeserializerMethod(data::mapping::type::__class::Int16::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int16>);
  setDeserializerMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt16>);

  setDeserializerMethod(data::mapping::type::__class::Int32::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int32>);
  setDeserializerMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt32>);

  setDeserializerMethod(data::mapping::type::__class::Int64::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int64>);
  setDeserializerMethod(data::mapping::type::__class::UInt64::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt64>);

  setDeserializerMethod(data::mapping::type::__class::Float32::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::Float64::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::Boolean::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::AbstractObject::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractList::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::AbstractPairList::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, nullptr);

}

void Deserializer::setDeserializerMethod(const data::mapping::type::ClassId& classId, DeserializerMethod method) {
  const v_uint32 id = classId.id;
  if(id < m_methods.size()) {
    m_methods[id] = method;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::setDeserializerMethod()]: Error. Unknown classId");
  }
}

oatpp::Void Deserializer::deserialize(const InData& data, Type* type) const {

  auto id = type->classId.id;
  auto& method = m_methods[id];

  if(method) {
    return (*method)(data, type);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserialize()]: "
                           "Error. No deserialize method for type '" + std::string(type->classId.name) + "'");

}

v_int16 Deserializer::deInt2(const InData& data) {
  if(data.size != 2) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deInt2()]: "
                             "Error. Invalid size for Int2 (v_int8)");
  }
  return ntohs(*((p_int16) data.data));
}

v_int32 Deserializer::deInt4(const InData& data) {
  if(data.size != 4) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deInt4()]: "
                             "Error. Invalid size for Int4 (v_int32)");
  }
  return ntohl(*((p_int32) data.data));
}

v_int64 Deserializer::deInt8(const InData& data) {

  if(data.size != 8) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deInt8()]: "
                             "Error. Invalid size for Int8 (v_int64)");
  }

  v_int64 l1 = ntohl(*((p_int32) data.data));
  v_int64 l2 = ntohl(*((p_int32) (data.data + 4)));

  return (l1 << 32) | l2 ;

}

v_int64 Deserializer::deInt(const InData& data) {
  switch(data.oid) {
    case INT2OID: return deInt2(data);
    case INT4OID: return deInt4(data);
    case INT8OID: return deInt8(data);
  }
  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deInt()]: Error. Unknown OID.");
}

oatpp::Void Deserializer::deserializeString(const InData& data, Type* type) {
  (void) type;
  switch(data.oid) {
    case TEXTOID: return oatpp::String(data.data, data.size, true);
  }
  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeString()]: Error. Unknown OID.");
}

}}}
