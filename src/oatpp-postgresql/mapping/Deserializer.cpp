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

#include "Deserializer.hpp"

#include "Oid.hpp"
#include "PgArray.hpp"
#include "oatpp-postgresql/Types.hpp"

namespace oatpp { namespace postgresql { namespace mapping {

Deserializer::InData::InData(PGresult* dbres, int row, int col, const std::shared_ptr<const data::mapping::TypeResolver>& pTypeResolver) {
  typeResolver = pTypeResolver;
  oid = PQftype(dbres, col);
  size = PQgetlength(dbres, row, col);
  data = PQgetvalue(dbres, row, col);
  isNull = PQgetisnull(dbres, row, col) == 1;
}

Deserializer::Deserializer() {

  m_methods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setDeserializerMethod(data::mapping::type::__class::String::CLASS_ID, &Deserializer::deserializeString);
  setDeserializerMethod(data::mapping::type::__class::Any::CLASS_ID, &Deserializer::deserializeAny);

  setDeserializerMethod(data::mapping::type::__class::Int8::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int8>);
  setDeserializerMethod(data::mapping::type::__class::UInt8::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt8>);

  setDeserializerMethod(data::mapping::type::__class::Int16::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int16>);
  setDeserializerMethod(data::mapping::type::__class::UInt16::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt16>);

  setDeserializerMethod(data::mapping::type::__class::Int32::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int32>);
  setDeserializerMethod(data::mapping::type::__class::UInt32::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt32>);

  setDeserializerMethod(data::mapping::type::__class::Int64::CLASS_ID, &Deserializer::deserializeInt<oatpp::Int64>);
  setDeserializerMethod(data::mapping::type::__class::UInt64::CLASS_ID, &Deserializer::deserializeInt<oatpp::UInt64>);

  setDeserializerMethod(data::mapping::type::__class::Float32::CLASS_ID, &Deserializer::deserializeFloat32);
  setDeserializerMethod(data::mapping::type::__class::Float64::CLASS_ID, &Deserializer::deserializeFloat64);
  setDeserializerMethod(data::mapping::type::__class::Boolean::CLASS_ID, &Deserializer::deserializeBoolean);

  setDeserializerMethod(data::mapping::type::__class::AbstractObject::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractEnum::CLASS_ID, &Deserializer::deserializeEnum);

  setDeserializerMethod(data::mapping::type::__class::AbstractVector::CLASS_ID, &Deserializer::deserializeArray);
  setDeserializerMethod(data::mapping::type::__class::AbstractList::CLASS_ID, &Deserializer::deserializeArray);
  setDeserializerMethod(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, &Deserializer::deserializeArray);

  setDeserializerMethod(data::mapping::type::__class::AbstractPairList::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, nullptr);

  ////

  setDeserializerMethod(postgresql::mapping::type::__class::Uuid::CLASS_ID, &Deserializer::deserializeUuid);

}

void Deserializer::setDeserializerMethod(const data::mapping::type::ClassId& classId, DeserializerMethod method) {
  const v_uint32 id = classId.id;
  if(id >= m_methods.size()) {
    m_methods.resize(id + 1, nullptr);
  }
  m_methods[id] = method;
}

oatpp::Void Deserializer::deserialize(const InData& data, const Type* type) const {

  auto id = type->classId.id;
  auto& method = m_methods[id];

  if(method) {
    return (*method)(this, data, type);
  }

  auto* interpretation = type->findInterpretation(data.typeResolver->getEnabledInterpretations());
  if(interpretation) {
    return interpretation->fromInterpretation(deserialize(data, interpretation->getInterpretationType()));
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
    case TIMESTAMPOID: return deInt8(data);
  }
  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deInt()]: Error. Unknown OID.");
}

oatpp::Void Deserializer::deserializeString(const Deserializer* _this, const InData& data, const Type* type) {

  (void) _this;
  (void) type;

  if(data.isNull) {
    return oatpp::String();
  }

  switch(data.oid) {
    case TEXTOID:
    case VARCHAROID: return oatpp::String(data.data, data.size);
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeString()]: Error. Unknown OID.");

}

oatpp::Void Deserializer::deserializeFloat32(const Deserializer* _this, const InData& data, const Type* type) {

  (void) _this;
  (void) type;

  if(data.isNull) {
    return oatpp::Float32();
  }

  switch(data.oid) {
    case FLOAT4OID: {
      v_int32 intVal = deInt4(data);
      return oatpp::Float32(*((p_float32) &intVal));
    }
    case FLOAT8OID: {
      v_int64 intVal = deInt8(data);
      return oatpp::Float32(*((p_float64) &intVal));
    }
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeFloat32()]: Error. Unknown OID.");

}

oatpp::Void Deserializer::deserializeFloat64(const Deserializer* _this, const InData& data, const Type* type) {

  (void) _this;
  (void) type;

  if(data.isNull) {
    return oatpp::Float64();
  }

  switch(data.oid) {
    case FLOAT4OID: {
      v_int32 intVal = deInt4(data);
      return oatpp::Float64(*((p_float32) &intVal));
    }
    case FLOAT8OID: {
      v_int64 intVal = deInt8(data);
      return oatpp::Float64(*((p_float64) &intVal));
    }
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeFloat32()]: Error. Unknown OID.");

}

oatpp::Void Deserializer::deserializeBoolean(const Deserializer* _this, const InData& data, const Type* type) {

  (void) _this;
  (void) type;

  if(data.isNull) {
    return oatpp::Boolean();
  }

  switch(data.oid) {
    case BOOLOID: return oatpp::Boolean((bool) data.data[0]);
    case INT2OID:
    case INT4OID:
    case INT8OID: return oatpp::Boolean((bool) deInt(data));
  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeBoolean()]: Error. Unknown OID.");

}

oatpp::Void Deserializer::deserializeEnum(const Deserializer* _this, const InData& data, const Type* type) {

  auto polymorphicDispatcher = static_cast<const data::mapping::type::__class::AbstractEnum::PolymorphicDispatcher*>(
    type->polymorphicDispatcher
  );

  data::mapping::type::EnumInterpreterError e = data::mapping::type::EnumInterpreterError::OK;
  const auto& value = _this->deserialize(data, polymorphicDispatcher->getInterpretationType());

  const auto& result = polymorphicDispatcher->fromInterpretation(value, e);

  if(e == data::mapping::type::EnumInterpreterError::OK) {
    return result;
  }

  switch(e) {
    case data::mapping::type::EnumInterpreterError::CONSTRAINT_NOT_NULL:
      throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeEnum()]: Error. Enum constraint violated - 'NotNull'.");

    default:
      throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeEnum()]: Error. Can't deserialize Enum.");
  }

}

const oatpp::Type* Deserializer::guessAnyType(const InData& data) {

  switch(data.oid) {

    case TEXTOID:
    case VARCHAROID: return oatpp::String::Class::getType();

    case INT2OID: return oatpp::Int16::Class::getType();
    case INT4OID: return oatpp::Int32::Class::getType();
    case INT8OID: return oatpp::Int64::Class::getType();

    case FLOAT4OID: return oatpp::Float32::Class::getType();
    case FLOAT8OID: return oatpp::Float64::Class::getType();

    case BOOLOID: return oatpp::Boolean::Class::getType();

    case TIMESTAMPOID: return oatpp::UInt64::Class::getType();

    case UUIDOID: return oatpp::postgresql::Uuid::Class::getType();

    // Arrays

    case TEXTARRAYOID:
    case VARCHARARRAYOID: return generateMultidimensionalArrayType<oatpp::String>(data);

    case INT2ARRAYOID: return generateMultidimensionalArrayType<oatpp::Int16>(data);
    case INT4ARRAYOID: return generateMultidimensionalArrayType<oatpp::Int32>(data);
    case INT8ARRAYOID: return generateMultidimensionalArrayType<oatpp::Int64>(data);

    case FLOAT4ARRAYOID: return generateMultidimensionalArrayType<oatpp::Float32>(data);
    case FLOAT8ARRAYOID: return generateMultidimensionalArrayType<oatpp::Float64>(data);

    case BOOLARRAYOID: return generateMultidimensionalArrayType<oatpp::Boolean>(data);

    case TIMESTAMPARRAYOID: return generateMultidimensionalArrayType<oatpp::UInt64>(data);

    case UUIDARRAYOID: return generateMultidimensionalArrayType<oatpp::postgresql::Uuid>(data);

  }

  return nullptr;
}

oatpp::Void Deserializer::deserializeAny(const Deserializer* _this, const InData& data, const Type* type) {

  (void) type;

  if(data.isNull) {
    return oatpp::Any();
  }

  const Type* valueType = guessAnyType(data);
  if(valueType == nullptr) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeAny()]: Error. Unknown OID.");
  }

  auto value = _this->deserialize(data, valueType);
  auto anyHandle = std::make_shared<data::mapping::type::AnyHandle>(value.getPtr(), value.getValueType());

  return oatpp::Void(anyHandle, Any::Class::getType());
}

oatpp::Void Deserializer::deserializeUuid(const Deserializer* _this, const InData& data, const Type* type) {

  (void) _this;
  (void) type;

  if(data.isNull) {
    return oatpp::postgresql::Uuid();
  }

  return postgresql::Uuid((p_char8)data.data);

}

oatpp::Void Deserializer::deserializeSubArray(const Type* type,
                                              ArrayDeserializationMeta& meta,
                                              v_int32 dimension)
{

  if(!type->isCollection) {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeSubArray()]: "
                             "Error. Unknown collection type.");
  }

  auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(type->polymorphicDispatcher);
  auto itemType = dispatcher->getItemType();
  auto collection = dispatcher->createObject();

  if(dimension < meta.dimensions.size() - 1) {

    auto size = meta.dimensions[dimension];

    for(v_int32 i = 0; i < size; i ++) {
      const auto& item = deserializeSubArray(itemType, meta, dimension + 1);
      dispatcher->addItem(collection, item);
    }

    return collection;

  } else if(dimension == meta.dimensions.size() - 1) {

    auto size = meta.dimensions[dimension];

    for(v_int32 i = 0; i < size; i ++) {

      v_int32 dataSize;
      meta.stream.readSimple(&dataSize, sizeof(v_int32));

      InData itemData;
      itemData.typeResolver = meta.data->typeResolver;
      itemData.size = (v_int32) ntohl(dataSize);
      itemData.data = (const char*) &meta.stream.getData()[meta.stream.getCurrentPosition()];
      itemData.oid = meta.arrayHeader.oid;
      itemData.isNull = itemData.size < 0;

      if(itemData.size > 0) {
        meta.stream.setCurrentPosition(meta.stream.getCurrentPosition() + itemData.size);
      }

      const auto& item = meta._this->deserialize(itemData, itemType);

      dispatcher->addItem(collection, item);

    }

    return collection;

  }

  throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserializeSubArray()]: "
                           "Error. Invalid state: dimension >= dimensions.size().");


}

oatpp::Void Deserializer::deserializeArray(const Deserializer* _this, const InData& data, const Type* type) {

  if(data.isNull) {
    return oatpp::Void(type);
  }

  auto ndim = (v_int32) ntohl(*((p_int32)data.data));
  if(ndim == 0) {
    auto dispatcher = static_cast<const data::mapping::type::__class::Collection::PolymorphicDispatcher*>(type->polymorphicDispatcher);
    return dispatcher->createObject(); // empty array
  }

  ArrayDeserializationMeta meta(_this, &data);
  return deserializeSubArray(type, meta, 0);

}

}}}
