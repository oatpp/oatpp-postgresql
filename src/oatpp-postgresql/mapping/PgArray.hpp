//
// Created by dsmyth on 1/1/21.
//

#ifndef oatpp_postgresql_mapping_PgArray_hpp
#define oatpp_postgresql_mapping_PgArray_hpp

// after https://stackoverflow.com/questions/4016412/postgresqls-libpq-encoding-for-binary-transport-of-array-data
struct PgArray {
    int32_t ndim;   // Number of dimensions
    int32_t _ign;   // offset for data, removed by libpq
    Oid oid;        // type of element in the array

    // Start of array (1st dimension)
    int32_t size;   // Number of elements
    int32_t index;  // Index of first element
    int32_t elem;   // Beginning of (size, value) elements
};

#endif // oatpp_postgresql_mapping_PgArray_hpp
