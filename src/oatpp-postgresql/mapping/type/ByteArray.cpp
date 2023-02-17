/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present
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

#include "ByteArray.hpp"

#include "oatpp/core/data/stream/BufferStream.hpp"
#include "oatpp/encoding/Base64.hpp"
#include "oatpp/encoding/Hex.hpp"

namespace oatpp {
namespace postgresql {
namespace mapping {
namespace type {

namespace __class {
const ClassId ByteArray::CLASS_ID("oatpp::postgresql::ByteArray");
}

ByteArray::ByteArray(const std::shared_ptr<std::vector<v_uint8>> &ptr, const _oatpp_Type *const valueType)
    : oatpp::data::mapping::type::ObjectWrapper<std::vector<v_uint8>, __class::ByteArray>(ptr) {
    if (type::__class::ByteArray::getType() != valueType) {
        throw std::runtime_error("Value type does not match");
    }
}

ByteArray ByteArray::fromHexEncodedString(const String &hexEncodedString) {
    const v_buff_usize expected_bytes_number = hexEncodedString->size() / 2;

    data::stream::BufferOutputStream stream(default_buffer_size);
    encoding::Hex::decode(&stream, hexEncodedString->data(), hexEncodedString->size(), true);
    if (stream.getCurrentPosition() != expected_bytes_number) {
        throw std::invalid_argument("[oatpp::postgresql::mapping::type::ByteArray::fromHexEncodedString(String)]:"
                                    "Error. Invalid string.");
    }

    return ByteArray((const v_uint8 *)stream.getData(), expected_bytes_number);
}

ByteArray ByteArray::fromBase64EncodedString(const String &base64EncodedString) {
    const auto tb = encoding::Base64::decode(base64EncodedString);
    return ByteArray(reinterpret_cast<const v_uint8 *>(tb->data()), tb->size());
}

String toBase64EncodedString(const ByteArray &arr) {

    if (arr->empty()) {
        return String();
    }
    return encoding::Base64::encode(arr->data(), arr->size());
}

String toHexEncodedString(const ByteArray &arr) {

    if (arr->empty()) {
        return String();
    }

    oatpp::data::stream::BufferOutputStream stream(ByteArray::default_buffer_size);
    encoding::Hex::encode(&stream, arr->data(), arr->size());

    return stream.toString();
}
} // namespace type
} // namespace mapping
} // namespace postgresql
} // namespace oatpp
