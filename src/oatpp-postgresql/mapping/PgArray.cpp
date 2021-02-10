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

#include "PgArray.hpp"

#if defined(WIN32) || defined(_WIN32)
  #include <WinSock2.h>
#else
  #include <arpa/inet.h>
#endif

namespace oatpp { namespace postgresql { namespace mapping {

void ArrayUtils::writeArrayHeader(data::stream::ConsistentOutputStream* stream, Oid itemOid, const std::vector<v_int32>& dimensions) {

  // num dimensions
  v_int32 v = htonl(dimensions.size());
  stream->writeSimple(&v, sizeof(v_int32));

  // ignore
  v = 0;
  stream->writeSimple(&v, sizeof(v_int32));

  // oid
  v = htonl(itemOid);
  stream->writeSimple(&v, sizeof(v_int32));

  // size
  v = htonl(dimensions[0]);
  stream->writeSimple(&v, sizeof(v_int32));

  // index
  v = htonl(1);
  stream->writeSimple(&v, sizeof(v_int32));

  for(v_uint32 i = 1; i < dimensions.size(); i++) {
    v_int32 size = htonl(dimensions[i]);
    v_int32 index = htonl(1);
    stream->writeSimple(&size, sizeof(v_int32));
    stream->writeSimple(&index, sizeof(v_int32));
  }

}

void ArrayUtils::readArrayHeader(data::stream::InputStream* stream,
                                 PgArrayHeader& arrayHeader,
                                 std::vector<v_int32>& dimensions)
{

  // num dimensions
  v_int32 v;
  stream->readExactSizeDataSimple(&v, sizeof(v_int32));
  arrayHeader.ndim = ntohl(v);

  // ignore
  stream->readExactSizeDataSimple(&v, sizeof(v_int32));

  // oid
  stream->readExactSizeDataSimple(&v, sizeof(v_int32));
  arrayHeader.oid = ntohl(v);

  // size
  stream->readExactSizeDataSimple(&v, sizeof(v_int32));
  arrayHeader.size = ntohl(v);

  // index // ignore
  stream->readExactSizeDataSimple(&v, sizeof(v_int32));

  dimensions.push_back(arrayHeader.size);

  for(v_uint32 i = 1; i < arrayHeader.ndim; i++) {
    stream->readExactSizeDataSimple(&v, sizeof(v_int32));
    dimensions.push_back(ntohl(v));

    // index // ignore
    stream->readExactSizeDataSimple(&v, sizeof(v_int32));
  }

}

}}}
