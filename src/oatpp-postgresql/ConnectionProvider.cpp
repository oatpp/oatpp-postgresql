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

#include "ConnectionProvider.hpp"

namespace oatpp { namespace postgresql {

void ConnectionProvider::ConnectionInvalidator::invalidate(const std::shared_ptr<Connection> &resource) {
  (void) resource;
  //Do nothing.
}

ConnectionProvider::ConnectionProvider(const oatpp::String& connectionString)
  : m_invalidator(std::make_shared<ConnectionInvalidator>())
  , m_connectionString(connectionString)
{}

provider::ResourceHandle<Connection> ConnectionProvider::get() {

  auto handle = PQconnectdb(m_connectionString->c_str());

  if(PQstatus(handle) == CONNECTION_BAD) {
    std::string errMsg = PQerrorMessage(handle);
    PQfinish(handle);
    throw std::runtime_error("[oatpp::postgresql::ConnectionProvider::get()]: "
                             "Error. Can't connect. " + errMsg);
  }

  return provider::ResourceHandle<Connection>(std::make_shared<ConnectionImpl>(handle), m_invalidator);

}

async::CoroutineStarterForResult<const provider::ResourceHandle<Connection>&> ConnectionProvider::getAsync() {
  throw std::runtime_error("[oatpp::postgresql::ConnectionProvider::getAsync()]: Error. Not implemented!");
}

void ConnectionProvider::stop() {
  // DO nothing
}

}}
