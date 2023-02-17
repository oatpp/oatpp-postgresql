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

#ifndef oatpp_data_mapping_type_ByteArray_hpp
#define oatpp_data_mapping_type_ByteArray_hpp

#include "oatpp/core/Types.hpp"
#include "oatpp/core/data/mapping/type/Primitive.hpp"
#include "oatpp/core/data/mapping/type/Type.hpp"

#include <cassert>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <vector>

namespace oatpp {
namespace postgresql {
namespace mapping {
namespace type {

namespace __class {

/**
 * ByteArray Class.
 */
class ByteArray {
  public:
    /**
     * Class Id
     */
    static const ClassId CLASS_ID;

    static Type *getType() {
        static Type type(CLASS_ID);
        return &type;
    }
};
} // namespace __class

/**
 * Mapping - enables ByteArray is &id:type::ObjectWrapper; over `std::vector<v_uint8>`;
 */
class ByteArray : public oatpp::data::mapping::type::ObjectWrapper<std::vector<v_uint8>, __class::ByteArray> {
    using ObjectWrapper::ObjectWrapper;
    using _oatpp_Type = oatpp::data::mapping::type::Type;

  public:
    static constexpr v_buff_usize default_buffer_size = 1024U;

    ByteArray() = default;
    ~ByteArray() = default;

    ByteArray(const std::shared_ptr<std::vector<v_uint8>> &ptr, const _oatpp_Type *const valueType);

    explicit ByteArray(v_buff_usize size)
        : oatpp::data::mapping::type::ObjectWrapper<std::vector<v_uint8>, __class::ByteArray>(
              std::make_shared<std::vector<v_uint8>>(size, 0)) {}

    ByteArray(const v_uint8 *data, v_buff_usize size)
        : oatpp::data::mapping::type::ObjectWrapper<std::vector<v_uint8>, __class::ByteArray>(
              std::make_shared<std::vector<v_uint8>>(data, data + size * sizeof(v_uint8))) {}

    ByteArray(std::initializer_list<v_uint8> ilist)
        : oatpp::data::mapping::type::ObjectWrapper<std::vector<v_uint8>, __class::ByteArray>(
              std::make_shared<std::vector<v_uint8>>(ilist)) {}

    ByteArray &operator=(std::initializer_list<v_uint8> ilist) {
        this->m_ptr = std::make_shared<std::vector<v_uint8>>(ilist);
        return *this;
    }

    ByteArray(const ByteArray &other) = default;
    inline ByteArray &operator=(const ByteArray &other) {
        m_ptr = other.m_ptr;
        return *this;
    }

    ByteArray(ByteArray &&other) = default;
    inline ByteArray &operator=(ByteArray &&other) noexcept {
        m_ptr = std::move(other.m_ptr);
        return *this;
    }

    /**
     * @brief Creates ByteArray from the HEX encoded string
     *
     * @return ByteArray
     */
    static ByteArray fromHexEncodedString(const String &hexEncodedString);

    /**
     * @brief Creates ByteArray from the Base64 encoded string
     *
     * @return ByteArray
     */
    static ByteArray fromBase64EncodedString(const String &base64EncodedString);

    /**
     * @brief Create a shared object of empty ByteArray
     *
     * @return ByteArray
     */
    static ByteArray createShared() { return std::make_shared<std::vector<v_uint8>>(); }

    /**
     * @brief Get constant reference to underlying container
     *
     * @return const std::vector<v_uint8>&
     */
    const std::vector<v_uint8> &operator*() const {
        assert(this->m_ptr);
        return this->m_ptr.operator*();
    }

    inline ByteArray &operator=(const std::vector<v_uint8> &str) {
        m_ptr = std::make_shared<std::vector<v_uint8>>(str);
        return *this;
    }

    inline ByteArray &operator=(std::vector<v_uint8> &&str) {
        m_ptr = std::make_shared<std::vector<v_uint8>>(std::move(str));
        return *this;
    }

    inline bool operator==(const std::vector<v_uint8> &str) const {
        if (!m_ptr) {
            return false;
        }
        return *m_ptr == str;
    }

    inline bool operator!=(const std::vector<v_uint8> &str) const { return !operator==(str); }

    inline bool operator==(const ByteArray &other) const {
        if (!m_ptr) {
            return !other.m_ptr;
        }

        if (!other.m_ptr) {
            return false;
        }
        return *m_ptr == *other.m_ptr;
    }

    inline bool operator!=(const ByteArray &other) const { return !operator==(other); }
};

/**
 * @brief Converts ByteArray to the base64 encoded string
 *
 * @param b input ByteArray to convert to Base64 encoded string
 * @return String
 */
String toBase64EncodedString(const ByteArray &b);

/**
 * @brief Converts ByteArray to the string of symbols representing hex-encoded bytes
 *
 * @param b input ByteArray to convert to HEX encoded string
 * @return String
 */
String toHexEncodedString(const ByteArray &b);

} // namespace type
} // namespace mapping
} // namespace postgresql
} // namespace oatpp

#endif // oatpp_data_mapping_type_ByteArray_hpp
