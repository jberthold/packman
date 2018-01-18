/* Packing as a library:
 *
 * GHC functions linked into the C code we use
 *
 */

#include <Rts.h>
// This brings in a lot of declared functions.

// All these are internal functions of the GHC runtime. While their
// functionality is usually very stable, future versions might need to
// #ifdef-out or modify some of these declarations.


// Internal functions in the GHC runtime
extern char* info_type(StgClosure*);
extern char* info_type_by_ip(StgInfoTable*);

// Internal hash table implementation
typedef struct hashtable HashTable;
extern HashTable *allocHashTable(void);
extern void *lookupHashTable(HashTable *table, StgWord key);
extern void  insertHashTable(HashTable *table, StgWord key, void *data);
extern void *removeHashTable(HashTable *table, StgWord key, void *data);
extern void  freeHashTable(HashTable *table, void (*freeDataFun)(void *));

// Internal malloc wrapper functions
extern void *stgMallocBytes(int n, char *msg) GNUC3_ATTRIBUTE(__malloc__);
extern void  stgFree(void* p);

#ifdef DEBUG
extern void checkClosure(StgClosure*);
#endif

#if __GLASGOW_HASKELL__ >= 801
// we have to bring the internal HEAP_ALLOCED macro in scope
// to be able to deal with CONSTR_NOCAF (to detect static ones)
// This header file is taken from GHC 8.2 source code directly
# include "GhcHeapAlloc.h"
#endif
