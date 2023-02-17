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

#include "ByteArrayTypeTest.hpp"

#include "oatpp-postgresql/Types.hpp"

#include <array>

namespace oatpp {
namespace test {
namespace postgresql {
namespace types {

void ByteArrayTypeTest::onRun() {

    {
        OATPP_LOGI(TAG, "test default constructor...");
        oatpp::postgresql::ByteArray array;

        OATPP_ASSERT(!array);
        OATPP_ASSERT(array == nullptr);

        OATPP_ASSERT(array.get() == nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());

        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test empty ilist constructor...");
        oatpp::postgresql::ByteArray array({});

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == 0);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());
        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test constructor from shared_ptr to empty vector...");
        oatpp::postgresql::ByteArray array(std::make_shared<std::vector<v_uint8>>());

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == 0);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());
        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test constructor from shared_ptr to non-empty vector...");
        std::vector<v_uint8> v = {0xA, 0xB, 0xC, 0xD};
        oatpp::postgresql::ByteArray array(std::make_shared<std::vector<v_uint8>>(v));

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == v.size());

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());

        OATPP_ASSERT(array->at(0) == v.at(0));
        OATPP_ASSERT(array->at(v.size() - 1) == v.at(v.size() - 1));

        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test assign operator with non-empty vector...");
        oatpp::postgresql::ByteArray array = {0xA, 0xB, 0xC, 0xD};
        const auto arr_size = 4U;

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == arr_size);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());

        OATPP_ASSERT(array->at(0) == 0xA);
        OATPP_ASSERT(array->at(arr_size - 1) == 0xD);

        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test constructor creating some buffer initialized with zeroes...");
        const auto buf_size = 11U;
        oatpp::postgresql::ByteArray array(buf_size);

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == buf_size);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());

        for (auto i = 0; i < buf_size; i++) {
            OATPP_ASSERT(array->at(i) == 0);
        }

        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test constructor creating converting data from some array...");
        const auto buf_size = 5U;
        constexpr std::array<v_uint8, buf_size> buffer = {0x1, 0x2, 0x3, 0x4, 0x5};
        oatpp::postgresql::ByteArray array(buffer.data(), buf_size);

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == buf_size);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());

        for (auto i = 0; i < buf_size; i++) {
            OATPP_ASSERT(array->at(i) == i + 1);
        }

        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test createShared()...");
        auto array = oatpp::postgresql::ByteArray::createShared();

        OATPP_ASSERT(array);
        OATPP_ASSERT(array != nullptr);
        OATPP_ASSERT(array->size() == 0);

        OATPP_ASSERT(array.get() != nullptr);
        OATPP_ASSERT(array.getValueType() == oatpp::postgresql::ByteArray::Class::getType());
        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test copy-assignment operator...");
        oatpp::postgresql::ByteArray array1({});
        oatpp::postgresql::ByteArray array2;

        array2 = array1;

        OATPP_ASSERT(array1);
        OATPP_ASSERT(array2);

        OATPP_ASSERT(array1->size() == 0);
        OATPP_ASSERT(array2->size() == 0);

        OATPP_ASSERT(array1.get() == array2.get());

        array2->push_back(0xA);

        OATPP_ASSERT(array1->size() == 1);
        OATPP_ASSERT(array2->size() == 1);
        OATPP_ASSERT(array2->at(0) == 0xA);
        OATPP_ASSERT(array1->at(0) == 0xA);

        std::vector<v_uint8> v2{0xB, 0xC};
        array2 = v2;

        OATPP_ASSERT(array1->size() == 1);
        OATPP_ASSERT(array2->size() == 2);

        OATPP_ASSERT(array2->at(0) == 0xB);
        OATPP_ASSERT(array2->at(1) == 0xC);
        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test move-assignment operator...");
        oatpp::postgresql::ByteArray array1({});
        oatpp::postgresql::ByteArray array2;
        OATPP_ASSERT(!array2);

        array2 = std::move(array1);

        OATPP_ASSERT(!array1);
        OATPP_ASSERT(array2);
        OATPP_LOGI(TAG, "OK");
    }

    {
        OATPP_LOGI(TAG, "test toHexEncodedString method...");
        oatpp::postgresql::ByteArray array1(
            {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF});

        OATPP_ASSERT(array1);
        OATPP_ASSERT(array1->size() == 16);

        const auto s1 = toHexEncodedString(array1);
        OATPP_LOGI(TAG, "arr.toHexEncodedString returned %s", s1->c_str());
        OATPP_ASSERT(oatpp::String("000102030405060708090A0B0C0D0E0F") == s1);

        oatpp::postgresql::ByteArray array2(
            {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF});

        OATPP_ASSERT(array2);
        OATPP_ASSERT(array2->size() == 16);

        const auto s2 = toHexEncodedString(array2);
        OATPP_LOGI(TAG, "arr.toHexEncodedString returned %s", s2->c_str());
        OATPP_ASSERT(oatpp::String("AABBCCDDEEFF060708090A0B0C0D0E0F") == s2);
    }

    {
        OATPP_LOGI(TAG, "test fromHexEncodedString method...");
        const std::vector<uint8_t> ba_result = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
                                                0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF};
        const auto in_string = oatpp::String("000102030405060708090A0B0C0D0E0F");

        const auto a = oatpp::postgresql::ByteArray::fromHexEncodedString(in_string);

        OATPP_ASSERT(a);
        OATPP_ASSERT(a->size() == 16);
        OATPP_ASSERT(a == ba_result);
    }

    {
        OATPP_LOGI(TAG, "test toBase64EncodedString method...");
        oatpp::postgresql::ByteArray array1(
            {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF});

        OATPP_ASSERT(array1);
        OATPP_ASSERT(array1->size() == 16);

        const auto s1 = toBase64EncodedString(array1);
        OATPP_LOGI(TAG, "arr.toBase64EncodedString returned %s", s1->c_str());
        OATPP_ASSERT(oatpp::String("AAECAwQFBgcICQoLDA0ODw==") == s1);

        oatpp::postgresql::ByteArray array2({0xAA, 0xBB, 0xCC, 0xDD, 0xEE});

        OATPP_ASSERT(array2);
        OATPP_ASSERT(array2->size() == 5);

        const auto s2 = toBase64EncodedString(array2);
        OATPP_LOGI(TAG, "arr.toBase64EncodedString returned %s", s2->c_str());
        OATPP_ASSERT(oatpp::String("qrvM3e4=") == s2);
    }

    {
        OATPP_LOGI(TAG, "test fromBase64EncodedString method...");
        const std::vector<uint8_t> ba_result = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7,
                                                0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF};
        const auto in_string = oatpp::String("AAECAwQFBgcICQoLDA0ODw==");

        const auto a = oatpp::postgresql::ByteArray::fromBase64EncodedString(in_string);

        OATPP_ASSERT(a);
        OATPP_ASSERT(a->size() == ba_result.size());
        OATPP_ASSERT(a == ba_result);
    }
}

} // namespace types
} // namespace postgresql
} // namespace test
} // namespace oatpp
