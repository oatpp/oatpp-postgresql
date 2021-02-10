//
// Created by dsmyth on 1/1/21.
//

#ifndef oatpp_postgresql_mapping_PgArray_hpp
#define oatpp_postgresql_mapping_PgArray_hpp

#include "oatpp/core/data/stream/Stream.hpp"
#include "oatpp/core/Types.hpp"

#include <libpq-fe.h>

namespace oatpp { namespace postgresql { namespace mapping {

// after https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
struct PgArrayHeader {

  v_int32 ndim = 0;   // Number of dimensions
  //v_int32 _ign;   // offset for data, removed by libpq
  Oid oid = InvalidOid;        // type of element in the array

  // Start of array (1st dimension)
  v_int32 size = 0;   // Number of elements
  // v_int32 index;  // Index of first element

};

template<typename T, int dim>
struct MultidimensionalArray {

  typedef oatpp::Vector<typename MultidimensionalArray<T, dim - 1>::type> type;

  static const oatpp::Type *getClassType() {
    return type::Class::getType();
  }

};

template<typename T>
struct MultidimensionalArray<T, 0> {
  typedef T type;
};

class ArrayUtils {
public:

  static void writeArrayHeader(data::stream::ConsistentOutputStream* stream,
                               Oid itemOid,
                               const std::vector<v_int32>& dimensions);

  static void readArrayHeader(data::stream::InputStream* stream,
                              PgArrayHeader& arrayHeader,
                              std::vector<v_int32>& dimensions);

};

}}}

#endif // oatpp_postgresql_mapping_PgArray_hpp
