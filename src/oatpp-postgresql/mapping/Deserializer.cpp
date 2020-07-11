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

namespace oatpp { namespace postgresql { namespace mapping {

Deserializer::Deserializer() {

  m_methods.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setDeserializerMethod(data::mapping::type::__class::String::CLASS_ID, &Deserializer::deserializeString);
  setDeserializerMethod(data::mapping::type::__class::Any::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::Int8::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::UInt8::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::Int16::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::UInt16::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::Int32::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::UInt32::CLASS_ID, nullptr);

  setDeserializerMethod(data::mapping::type::__class::Int64::CLASS_ID, nullptr);
  setDeserializerMethod(data::mapping::type::__class::UInt64::CLASS_ID, nullptr);

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

void Deserializer::deserialize(const char** outData, const oatpp::Void& polymorph) const {
  auto id = polymorph.valueType->classId.id;
  auto& method = m_methods[id];
  if(method) {
    (*method)(outData, polymorph);
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::Deserializer::deserialize()]: "
                             "Error. No deserialize method for type '" + std::string(polymorph.valueType->classId.name) + "'");
  }
}

void Deserializer::deserializeString(const char** outData, const oatpp::Void& polymorph) {
  *outData = static_cast<base::StrBuffer*>(polymorph.get())->c_str();
}

}}}
