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

#include "TypeMapper.hpp"

#include "Oid.hpp"

namespace oatpp { namespace postgresql { namespace mapping {

TypeMapper::TypeMapper() {

  m_oids.resize(data::mapping::type::ClassId::getClassCount(), 0);

  setTypeOid(data::mapping::type::__class::String::CLASS_ID, TEXTOID);
  setTypeOid(data::mapping::type::__class::Any::CLASS_ID, 0);

  setTypeOid(data::mapping::type::__class::Int8::CLASS_ID, INT2OID);
  setTypeOid(data::mapping::type::__class::UInt8::CLASS_ID, INT2OID);

  setTypeOid(data::mapping::type::__class::Int16::CLASS_ID, INT2OID);
  setTypeOid(data::mapping::type::__class::UInt16::CLASS_ID, INT4OID);

  setTypeOid(data::mapping::type::__class::Int32::CLASS_ID, INT4OID);
  setTypeOid(data::mapping::type::__class::UInt32::CLASS_ID, INT8OID);

  setTypeOid(data::mapping::type::__class::Int64::CLASS_ID, INT8OID);
  setTypeOid(data::mapping::type::__class::UInt64::CLASS_ID, 0);

  setTypeOid(data::mapping::type::__class::Float32::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::Float64::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::Boolean::CLASS_ID, 0);

  setTypeOid(data::mapping::type::__class::AbstractObject::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::AbstractEnum::CLASS_ID, 0);

  setTypeOid(data::mapping::type::__class::AbstractVector::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::AbstractList::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::AbstractUnorderedSet::CLASS_ID, 0);

  setTypeOid(data::mapping::type::__class::AbstractPairList::CLASS_ID, 0);
  setTypeOid(data::mapping::type::__class::AbstractUnorderedMap::CLASS_ID, 0);

}

void TypeMapper::setTypeOid(const data::mapping::type::ClassId& classId, Oid oid) {
  const v_uint32 id = classId.id;
  if(id < m_oids.size()) {
    m_oids[id] = oid;
  } else {
    throw std::runtime_error("[oatpp::postgresql::mapping::TypeMapper::setTypeOid()]: Error. Unknown classId");
  }
}

Oid TypeMapper::getTypeOid(data::mapping::type::Type* type) const {
  const v_uint32 id = type->classId.id;
  if(id < m_oids.size()) {
    return m_oids[id];
  }
  throw std::runtime_error("[oatpp::postgresql::mapping::TypeMapper::getTypeOid()]: Error. Unknown classId");
}

}}}
