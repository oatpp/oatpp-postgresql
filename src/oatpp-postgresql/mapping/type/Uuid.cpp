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

#include "Uuid.hpp"
#include "oatpp/core/data/stream/BufferStream.hpp"

namespace oatpp { namespace postgresql { namespace mapping { namespace type {

namespace __class {
  const oatpp::ClassId Uuid::CLASS_ID("oatpp::postgresql::Uuid");
}

const char* const Uuid::ALPHABET = "0123456789abcdef";

Uuid::Uuid(v_char8 data[DATA_SIZE]) {
  std::memcpy(m_data, data, DATA_SIZE);
}

const p_char8 Uuid::getData() const {
  return (const p_char8) m_data;
}

v_buff_size Uuid::getSize() const {
  return DATA_SIZE;
}

oatpp::String Uuid::toString() const {
  oatpp::String result(DATA_SIZE * 2 + 4);
  p_char8 rdata = result->getData();
  v_int32 shift = 0;
  for(v_buff_size i = 0; i < DATA_SIZE; i ++) {
    if(i == 4 || i == 6 || i == 8 || i == 10) {
      rdata[shift + i * 2] = '-';
      ++ shift;
    }
    auto a = m_data[i];
    v_char8 b1 = 0x0F & (a >> 4);
    v_char8 b2 = 0x0F & (a);
    rdata[shift + i * 2    ] = ALPHABET[b1];
    rdata[shift + i * 2 + 1] = ALPHABET[b2];
  }
  return result;
}

bool Uuid::operator==(const Uuid &other) const {
  return std::memcmp(m_data, other.m_data, DATA_SIZE) == 0;
}

bool Uuid::operator!=(const Uuid &other) const {
  return !operator==(other);
}

}}}}
