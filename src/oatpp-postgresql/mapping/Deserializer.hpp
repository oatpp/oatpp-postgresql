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

#ifndef oatpp_postgresql_mapping_Deserializer_hpp
#define oatpp_postgresql_mapping_Deserializer_hpp

#include "oatpp/core/Types.hpp"

namespace oatpp { namespace postgresql { namespace mapping {

class Deserializer {
public:
  typedef void (*DeserializerMethod)(const char**, const oatpp::Void&);
private:
  std::vector<DeserializerMethod> m_methods;
public:

  Deserializer();

  void setDeserializerMethod(const data::mapping::type::ClassId& classId, DeserializerMethod method);

  void deserialize(const char** outData, const oatpp::Void& polymorph) const;

public:

  static void deserializeString(const char** outData, const oatpp::Void& polymorph);

};

}}}

#endif // oatpp_postgresql_mapping_Deserializer_hpp
