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

#include "TemplateValueProvider.hpp"

namespace oatpp { namespace postgresql { namespace ql_template {

TemplateValueProvider::TemplateValueProvider(const orm::Executor::ParamsTypeMap* paramsTypeMap)
  : m_paramsTypeMap(paramsTypeMap)
{
  m_typeNames.resize(data::mapping::type::ClassId::getClassCount(), nullptr);

  setTypeName(data::mapping::type::__class::String::CLASS_ID, "text");
  setTypeName(data::mapping::type::__class::Any::CLASS_ID, nullptr);

  setTypeName(data::mapping::type::__class::Int8::CLASS_ID, "int2");
  setTypeName(data::mapping::type::__class::UInt8::CLASS_ID, "int2");

  setTypeName(data::mapping::type::__class::Int16::CLASS_ID, "int2");
  setTypeName(data::mapping::type::__class::UInt16::CLASS_ID, "int4");

  setTypeName(data::mapping::type::__class::Int32::CLASS_ID, "int4");
  setTypeName(data::mapping::type::__class::UInt32::CLASS_ID, "int8");

  setTypeName(data::mapping::type::__class::Int64::CLASS_ID, "int8");
  setTypeName(data::mapping::type::__class::UInt64::CLASS_ID, nullptr);

  setTypeName(data::mapping::type::__class::Float32::CLASS_ID, "float4");
  setTypeName(data::mapping::type::__class::Float64::CLASS_ID, "float8");
  setTypeName(data::mapping::type::__class::Boolean::CLASS_ID, "bool");

  setTypeName(data::mapping::type::__class::AbstractObject::CLASS_ID, nullptr);
  setTypeName(data::mapping::type::__class::AbstractEnum::CLASS_ID, nullptr);

  setTypeName(data::mapping::type::__class::AbstractVector::CLASS_ID, nullptr);
  setTypeName(data::mapping::type::__class::AbstractList::CLASS_ID, nullptr);
  setTypeName(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, nullptr);

  setTypeName(data::mapping::type::__class::AbstractPairList::CLASS_ID, nullptr);
  setTypeName(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, nullptr);

}

void TemplateValueProvider::setTypeName(const data::mapping::type::ClassId& classId, const oatpp::String& name) {
  const v_uint32 id = classId.id;
  if(id < m_typeNames.size()) {
    m_typeNames[id] = name;
  } else {
    throw std::runtime_error("[oatpp::postgresql::ql_template::TemplateValueProvider::setTypeName()]: "
                             "Error. Unknown classId. Class-Name=" + std::string(classId.name));
  }
}

oatpp::String TemplateValueProvider::getValue(const data::share::StringTemplate::Variable& variable, v_uint32 index) {

  auto typeIt = m_paramsTypeMap->find(variable.name);
  if(typeIt == m_paramsTypeMap->end()) {
    throw std::runtime_error("[oatpp::postgresql::Executor::PGTemplateValueProvider::getValue()]: Error. "
                             "Type info not found for variable " + variable.name->std_str());
  }

  auto typeName = m_typeNames[typeIt->second->classId.id];

  if(!typeName) {
    throw std::runtime_error("[oatpp::postgresql::Executor::PGTemplateValueProvider::getValue()]: Error. "
                             "Unsupported type - " + std::string(typeIt->second->classId.name));
  }

  m_buffStream.setCurrentPosition(0);
  m_buffStream << "$" << (index + 1);// << "::" << typeName;

  return m_buffStream.toString();

}

}}}
