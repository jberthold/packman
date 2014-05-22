#ifndef PACKMAN_TYPES_H
#define PACKMAN_TYPES_H



// packing and sending:
// Pack Buffer for constructing messages between PEs
// defined here instead of in RtsTypes.h due to FLEXIBLE_ARRAY usage
typedef struct pmPackBuffer_ {
    // for data messages only,
    StgInt /* nat */     size;
    StgInt /* nat */     unpacked_size;
    StgWord              buffer[FLEXIBLE_ARRAY];
} pmPackBuffer;


#endif /* PACKMAN_TYPES_H */
