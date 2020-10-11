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

#include "oatpp/core/data/mapping/TypeResolver.hpp"
#include "oatpp/core/Types.hpp"

#include <libpq-fe.h>

namespace oatpp { namespace postgresql { namespace mapping {

class Deserializer {
public:

  struct InData {

    InData(PGresult* dbres, int row, int col, const std::shared_ptr<const data::mapping::TypeResolver>& pTypeResolver);

    std::shared_ptr<const data::mapping::TypeResolver> typeResolver;

    Oid oid;
    const char* data;
    v_buff_size size;
    bool isNull;

  };

public:
  typedef oatpp::Void (*DeserializerMethod)(const Deserializer*, const InData&, const Type*);
private:
  static v_int16 deInt2(const InData& data);
  static v_int32 deInt4(const InData& data);
  static v_int64 deInt8(const InData& data);
  static v_int64 deInt(const InData& data);

  static const oatpp::Type* guessAnyType(Oid oid);
private:
  std::vector<DeserializerMethod> m_methods;
public:

  Deserializer();

  void setDeserializerMethod(const data::mapping::type::ClassId& classId, DeserializerMethod method);

  oatpp::Void deserialize(const InData& data, const Type* type) const;

private:

  static oatpp::Void deserializeString(const Deserializer* _this, const InData& data, const Type* type);

  template<class IntWrapper>
  static oatpp::Void deserializeInt(const Deserializer* _this, const InData& data, const Type* type) {
    (void) _this;
    (void) type;
    if(data.isNull) {
      return IntWrapper();
    }
    auto value = deInt(data);
    return IntWrapper((typename IntWrapper::UnderlyingType) value);
  }

  static oatpp::Void deserializeFloat32(const Deserializer* _this, const InData& data, const Type* type);
  static oatpp::Void deserializeFloat64(const Deserializer* _this, const InData& data, const Type* type);

  static oatpp::Void deserializeBoolean(const Deserializer* _this, const InData& data, const Type* type);

  static oatpp::Void deserializeEnum(const Deserializer* _this, const InData& data, const Type* type);

  static oatpp::Void deserializeAny(const Deserializer* _this, const InData& data, const Type* type);

  static oatpp::Void deserializeUuid(const Deserializer* _this, const InData& data, const Type* type);

};

}}}

#endif // oatpp_postgresql_mapping_Deserializer_hpp
