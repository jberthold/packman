#ifndef PACKMAN_TYPES_H
#define PACKMAN_TYPES_H


// A port type, stands for an in-port (pe, proc,inport->id), an out-port
// (pe,proc,tso->id) and processes (pe, proc, NULL)
typedef struct Port_ {
    nat machine;
    StgWord process;
    StgWord id;
} Port;

typedef Port Proc;


// packing and sending:
// Pack Buffer for constructing messages between PEs
// defined here instead of in RtsTypes.h due to FLEXIBLE_ARRAY usage
typedef struct pmPackBuffer_ {
    // Eden channel communication
    Port                 sender;
    Port                 receiver;
    // for data messages only,  
    StgInt /* nat */     id; 
    StgInt /* nat */     size;
    StgInt /* nat */     unpacked_size;
    struct StgTSO_       *tso;
    StgWord              buffer[FLEXIBLE_ARRAY];
} pmPackBuffer;


#endif /* PACKMAN_TYPES_H */
