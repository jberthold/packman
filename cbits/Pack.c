/*
   Packing for the Generic RTE:
--------

   Graph packing and unpacking code for sending it to another processor
   and retrieving the original graph structure from the packet.
   Derived from RTS code used in GUM and Eden.

   Documentation for heap closures can be found at
   http://hackage.haskell.org/trac/ghc/wiki/Commentary/Rts/Storage/HeapObjects
   However, the best documentation is includes/rts/storage/Closure*h
   and rts/sm/Scav.c

   This file: a heavily revised version which uses a thread-local
   internal pack state to make the code thread-safe.

*/

#include <Rts.h>
#include <string.h>

#include "Types.h"
#include "Errors.h"
#include "GHCFunctions.h"

#ifdef DEBUG
#define DBG_HEADROOM 1
#define END_OF_BUFFER_MARKER 0xdededeee
#else
#define DBG_HEADROOM 0
#endif

// size of the (fixed) Closure header in words
#define HEADERSIZE sizeof(StgHeader)/sizeof(StgWord)

// markers for packed/unpacked type
#define PLC     1L
#define OFFSET  2L
#define CLOSURE 3L
// marker for small bitmap in PAP packing
#define SMALL_BITMAP_TAG (~0L)

/* Tagging macros will work for any word-sized type, not only
  closures. In the packet, we tag info pointers instead of
  closure pointers.
  See "pointer tagging" before "PackNearbyGraph" routine for use.
*/
#define UNTAG_CAST(type,p) ((type) UNTAG_CLOSURE((StgClosure*) (p)))

// Info pointer <--> Info offset (also for PLC pointers)
// See "relocatable binaries" before "PackNearbyGraph" routine for use.

// use this one on info pointers before they go into a packet
#define P_OFFSET(ip) ((StgWord) ((StgWord) (ip)) - (StgWord) BASE_SYM)
// use this one on info offsets taken from packets
#define P_POINTER(val) ((StgWord)(val) + (StgWord) BASE_SYM)

// padding for offsets into the already-packed data (failing lookup in the
// hashtable will produce 0, but offset 0 would be the graph root without
// padding)
#define PADDING 1

// internal types

// closure queue: array implementation with wrap-around
// Could use WSDeQue from the RTS, but we don't need its thread-safety
typedef struct ClosureQ_ {
    StgClosure** queue;
    nat size; // all in units of StgClosure*
    nat head;
    nat tail;
} ClosureQ;

// packing state: buffer, queue, offset table
typedef struct PackState_ {
    StgWord*  buffer;
    nat       size;     // buffer size in StgWords
    nat       position; // position in buffer, in StgWords
    ClosureQ  *queue;
    HashTable *offsets;
} PackState;

// forward declarations

// Init module at startup
static void pmInit(void) __attribute__((constructor));

// init/destruct pack data structure
PackState* initPacking(StgArrWords *mutArr);
void donePacking(PackState *state);

// closure queue
ClosureQ* initClosureQ(nat size);
void freeClosureQ(ClosureQ* q);
STATIC_INLINE rtsBool queueEmpty(ClosureQ* q);
STATIC_INLINE nat queueSize(ClosureQ* q);
void queueClosure(ClosureQ* q, StgClosure *closure);
StgClosure *deQueueClosure(ClosureQ* q);

// packing

// little helpers:
STATIC_INLINE void registerOffset(PackState* p, StgClosure *closure);
STATIC_INLINE StgWord offsetFor(PackState* p, StgClosure *closure);
STATIC_INLINE rtsBool roomToPack(PackState* p, nat size);

STATIC_INLINE rtsBool isBlackhole(StgClosure* node);
STATIC_INLINE StgInfoTable* getClosureInfo(StgClosure* node, StgInfoTable* info,
                                           nat *size, nat *ptrs,
                                           nat *nonptrs, nat *vhs);

// interface function used in foreign primop
int pmtryPackToBuffer(StgClosure* closure, StgArrWords* mutArr);

// packing static addresses and offsets
STATIC_INLINE void PackPLC(PackState* p, StgPtr addr);
STATIC_INLINE void PackOffset(PackState* p, StgWord offset);
// packing routine, branches into special cases
StgWord packClosure(PackState* p, StgClosure *closure);

// low-level packing: fill one StgWord of data into the globalPackBuffer
STATIC_INLINE void Pack(PackState* p, StgWord data);

// the workhorses: generic heap-alloc'ed (ptrs-first) closure
static StgWord PackGeneric(PackState* p, StgClosure *closure);
// and special cases
static StgWord PackPAP(PackState* p, StgPAP *pap);
static StgWord PackArray(PackState* p, StgClosure* array);

// unpacking

// interface unpacking from a Haskell array (using the Haskell Byte Array)
// may return error code P_GARBLED
StgClosure* pmUnpackGraphWrapper(StgArrWords* packBufferArray, Capability* cap);

// internal function working on the raw data buffer
StgClosure* pmUnpackGraph_(StgWord *buffer, StgInt size, Capability* cap);

// helper function to find next pointer (filling in pointers)
STATIC_INLINE void locateNextParent(ClosureQ* q, StgClosure **parentP,
                                    nat* pptrP, nat* pptrsP, nat* pvhsP);

// core unpacking function
static  StgClosure* UnpackClosure (ClosureQ* q, HashTable* offsets,
                                   StgWord **bufptrP, Capability* cap);

// helpers
STATIC_INLINE StgClosure *UnpackOffset(HashTable* offsets, StgWord **bufptrP);
STATIC_INLINE  StgClosure *UnpackPLC(StgWord **bufptrP);
static StgClosure * UnpackPAP(ClosureQ *queue, StgInfoTable *info,
                              StgWord **bufptrP, Capability* cap);
static StgClosure* UnpackArray(ClosureQ *queue, StgInfoTable* info,
                               StgWord **bufptrP, Capability* cap);


#if defined(DEBUG)
// finger print: "type hash" of packed graph, for quick debugging
// checks
#define MAX_FINGER_PRINT_LEN  1023
static void graphFingerPrint(char* fp, StgClosure *graphroot);

void pmcheckPacket(StgWord* buffer, nat size);
#endif

/***************************************************************
 * init function (called when loading the module)
 */
static void pmInit(void) {
    // we must retain all CAFs, as packet data might refer to it.
    // This variable lives in Storage.c, inhibits GC for CAFs.
    keepCAFs = rtsTrue;
}

/***************************************************************
 * pack state and queue functions
 */

// Pack state constructor, allocates space, queue and hash table. A mutable
// array is passed as the buffer space. Note that its size comes in bytes, while
// internally all is managed in units of StgWord.
PackState* initPacking(StgArrWords *mutArr) {
    PackState *ret;

    ret = (PackState*) stgMallocBytes(sizeof(PackState), "pack state");

    ASSERT(mutArr->bytes > 0);

    ret->buffer = mutArr->payload;
    ret->size = mutArr->bytes / sizeof(StgWord);
    ret->position = 0;
    // create a closure queue "big enough" => about what the array can hold
    ret->queue = initClosureQ(ret->size / 2);
    // new hash table
    ret->offsets = allocHashTable();

    return ret;
}

// Pack state destructor: frees hashtable and queue. Mutable array used when
// initialising has now been mutated.
void donePacking(PackState *state) {
    freeHashTable(state->offsets, NULL);
    freeClosureQ(state->queue);
    stgFree(state);
    return;
}

// initialise a closure queue for "size" many closures
ClosureQ* initClosureQ(nat size) {
    ClosureQ* ret;
    ret = (ClosureQ*) stgMallocBytes(sizeof(ClosureQ), "cl.queue");
    ret->queue = (StgClosure**)
        stgMallocBytes(size * sizeof(StgClosure*), "cl.queue data");
    ret->size = size;
    ret->head = ret->tail = 0;
    return ret;
}

// free an allocated closure queue
void freeClosureQ(ClosureQ* q) {
    stgFree(q->queue);
    stgFree(q);
}

// queue empty if head == tail
STATIC_INLINE rtsBool queueEmpty(ClosureQ* q) {
    return (q->head == q->tail);
}

// compute size from distance between head and tail (with wrap-around)
STATIC_INLINE nat queueSize(ClosureQ* q) {
    // queue can wrap around
    int span = q->head - q->tail;

    ASSERT(span < q->size);

    if (span >= 0) {
        return (nat) span;
    } else {
        // wrapped around
        return (q->size - span);
    }
}

// enqueue a closure
void queueClosure(ClosureQ* q, StgClosure *closure) {

    // next index, wrapping around when required
    nat idx = (q->head == q->size - 1) ? 0 : q->head + 1;

    if (idx == q->tail) {
        // queue full, stop (should not happen with sizes used here)
        errorBelch("Pack.c: Closure Queue Overflow");
        stg_exit(EXIT_FAILURE);
    }
    q->queue[q->head] = closure;
    IF_DEBUG(sparks,
             debugBelch(">__> Q: %p (%s) at %ld\n", closure,
                        info_type(UNTAG_CLOSURE(closure)), (long) q->head));
    q->head = idx;

}

// dequeue a closure
StgClosure *deQueueClosure(ClosureQ* q) {
    if (!queueEmpty(q)) {
        StgClosure* c = q->queue[q->tail];
        q->tail = (q->tail == q->size-1) ? 0 : (q->tail + 1);
        IF_DEBUG(sparks,
                debugBelch(">__> DeQ: %p (%s); %ld elems in q\n",
                           c, info_type(UNTAG_CLOSURE(c)),
                           (long) queueSize(q)));
        return c;
    } else {
        IF_DEBUG(sparks, debugBelch("Q empty\n "));
        return ((StgClosure*)NULL);
    }
}



/***************************************************************
 * Helper functions for packing
 */

// RegisterOffset records that/where the closure is packed
STATIC_INLINE void registerOffset(PackState* p, StgClosure *closure) {
    insertHashTable(p->offsets, UNTAG_CAST(StgWord, closure),
                    // remove tag for offset
                    (void *) (StgWord) (p->position + PADDING));
    // note: offset is never 0 (indicates failing lookup), PADDING is 1
}

// OffsetFor returns an offset for a closure which has already been packed.
// offsetFor returns 0 => closure has _not_ been packed
// (root closure gets offset 1, see PADDING above)
STATIC_INLINE StgWord offsetFor(PackState* p, StgClosure *closure) {
    // avoid typecast warnings...
    void* offset;
    offset = lookupHashTable(p->offsets, UNTAG_CAST(StgWord, closure));
                             // remove tag for offset
    return (StgWord) offset;
}

// roomToPack checks if the buffer has enough space to pack the given size (in
// StgWords). For GUM, it would also include queue size * FETCHME-size.
STATIC_INLINE rtsBool roomToPack(PackState* p, nat size)
{
    if ((p->position +  // where we are in the buffer right now
         size +         // space needed for the current closure
#ifdef GUM
         queueSize(q) * FETCH_ME_PACKED_SIZE +
#endif
         1)             // closure tag
        >= p->size) {
        IF_DEBUG(prof,
                 debugBelch("Pack buffer full (size %d). ", p->position));
        return rtsFalse;
    }
    return rtsTrue;
}

// quick test for blackholes. Available somewhere else?
STATIC_INLINE rtsBool isBlackhole(StgClosure* node) {
    // since ghc-7.0, blackholes are used as indirections. inspect indirectee.
    if(((StgInfoTable*)get_itbl(UNTAG_CLOSURE(node)))->type == BLACKHOLE) {
        StgClosure* indirectee = ((StgInd*)node)->indirectee;
        // some Blackholes are actually indirections since ghc-7.0
        switch (((StgInfoTable*)get_itbl(UNTAG_CLOSURE(indirectee)))->type) {
        case TSO:
        case BLOCKING_QUEUE:
            return rtsTrue;
        default:
            return rtsFalse;
        }
    }
    return rtsFalse;
}

// unwind (chains of) indirections, return the actual data closure
// Blackholes are one kind of indirection, see above.
STATIC_INLINE StgClosure* unwindInd(StgClosure *closure)
{
    StgClosure *start = closure;

    while (closure_IND(start))
        start = (UNTAG_CAST(StgInd*, start))->indirectee;

    return start;
}

/***************************************************************
 * general helper function used here
 */

/*  getClosureInfo: returns payload structure for closures.
    Only used in here
    IN:  node   - ptr to the closure / into the packet
         info   - (optional) info _table_ (not info ptr!) for closure
                  to be computed from info offset by caller when in packet
    OUT: size   - total size of closure in heap (for allocation)
         ptrs   - number of pointers in payload
         nonptrs- number of non-pointers in payload
         vhs    - variable header size
    RETURNS: info _table_ (pointer)
*/
STATIC_INLINE StgInfoTable*
getClosureInfo(StgClosure* node, StgInfoTable* info,
                 nat *size, nat *ptrs, nat *nonptrs, nat *vhs) {
    // We remove the potential tag before doing anything.
    node = UNTAG_CLOSURE(node);

    if (info == NULL) {
        // Supposed to compute info table by ourselves. This will go very wrong
        // if we use an info _offset_ instead (if we are supposed to look at a
        // packet slot instead of the heap) which is the case if we find
        // something tagged.
        ASSERT(!GET_CLOSURE_TAG((StgClosure*) node->header.info));
        // not tagged, OK
        info = get_itbl(node);
    }
    // ClosureMacros.h. NB relies on variable header for PAP, AP, Arrays
    *size = closure_sizeW_(node, info);

    /* Caution: layout field is union, may contain different information
       according to closure type! see InfoTables.h: THUNK_SELECTOR:
       selector_offset stack frames, ret. vec.s, whatever: bitmap / ptr. to
       large_bitmap other closures: ptrs | nptrs
       */
    switch (info->type) {
    case THUNK_SELECTOR:
        *ptrs = 1; // selectee is a pointer
        *vhs  = *size - 1 - sizeofW(StgHeader);
        *nonptrs = 0;
        break;

        // PAP/AP/AP_STACK contain a function field,
        // treat this field as a (= the one single) pointer
    case PAP:
        *vhs = 1; // arity/args
        *ptrs = 1;
        // wrong (some are ptrs), but not used in the unpacking code!
        *nonptrs = *size - 2 - sizeofW(StgHeader);
        break;

    case AP_STACK:
    case AP:
        // thunk header and arity/args field
        *vhs = sizeofW(StgThunkHeader) - sizeofW(StgHeader) + 1;
        *ptrs = 1;
        // wrong (some are ptrs), but not used in the unpacking code!
        *nonptrs = *size - 2 - sizeofW(StgThunkHeader);
        break;

        /* For Word arrays, no pointers need to be filled in.
         * (the generic "ptrs-first" treatment works for them)
         */
    case ARR_WORDS:
        *vhs = 1;
        *ptrs = 0;
        *nonptrs = (((StgArrWords*) node)->bytes) / sizeof(StgWord);
        break;

        /* For Arrays of pointers, we need to fill in all the pointers and
           allocate additional space for the card table at the end.
        */
    case MUT_ARR_PTRS_CLEAN:
    case MUT_ARR_PTRS_DIRTY:
    case MUT_ARR_PTRS_FROZEN0:
    case MUT_ARR_PTRS_FROZEN:
        *vhs = 2;
        *ptrs = ((StgMutArrPtrs*) node)->ptrs;
        *nonptrs = ((StgMutArrPtrs*) node)->size - *ptrs; // count card table
        // NB nonptrs field for array closures is only used in checkPacket
        break;

#if __GLASGOW_HASKELL__ > 708
        // Small arrays do not have card tables, straightforward
    case SMALL_MUT_ARR_PTRS_CLEAN:
    case SMALL_MUT_ARR_PTRS_DIRTY:
    case SMALL_MUT_ARR_PTRS_FROZEN0:
    case SMALL_MUT_ARR_PTRS_FROZEN:
        *vhs = 1; // ptrs field
        *ptrs = ((StgSmallMutArrPtrs*) node)->ptrs;
        *nonptrs = 0;
        break;
#endif

        // we do not want to see these here (until thread migration)
    case CATCH_STM_FRAME:
    case CATCH_RETRY_FRAME:
    case ATOMICALLY_FRAME:
    case UPDATE_FRAME:
    case CATCH_FRAME:
    case UNDERFLOW_FRAME:
    case STOP_FRAME:
    case RET_SMALL:
    case RET_BIG:
    case RET_BCO:
        barf("getClosureInfo: stack frame!");
        break;

    default:
        // this works for all pointers-first layouts
        *ptrs = (nat) (info->layout.payload.ptrs);
        *nonptrs = (nat) (info->layout.payload.nptrs);
        *vhs = *size - *ptrs - *nonptrs - sizeofW(StgHeader);
    }

    ASSERT(*size == sizeofW(StgHeader) + *vhs + *ptrs + *nonptrs);

    return info;
}

/*******************************************************************
 * packing a graph structure:
 *
 * The graph is packed breadth-first into a given buffer of StgWords.
 *
 * In the buffer, 3 different types of entities are packed
 *  0L - closure with static address        - PackPLC
 *  1L - offset (closure already in packet) - PackOffset
 *  2L - a heap closure follows             - PackGeneric/specialised routines
 *
 *  About "pointer tagging":
 *   Every closure pointer carries a tag in its l.s. bits (those which
 *   are not needed since closures are word-aligned anyway).
 *   These tags indicate that data pointed at is fully evaluated, and
 *   allow for a shortcut in case of small constructors (selecting
 *   arguments).
 *   The tagged pointers are *references* to a closure. RTS must ensure
 *   that every occurrence of one and the same pointer has the same tag.
 *
 *   Tags should survive packing-sending-unpacking, so we must store
 *   them in the packet somehow.
 *   => Closure pointers in the closure queue are stored *WITH TAGS*,
 *   and we pack the tags together with the closure.
 *   OTOH, *offsets* (HashTable entries) are stored without tags, in
 *   order to catch the case when two references with different tags
 *   exist (possible?)
 *   (the tag of the first occurrence will win, a problem?)
 *
 *   We use the last bits of the info-ptr (stored anyway), which is
 *   aligned just as closure pointers, in word size.
 *
 *   Anyway, closures are enqueued with tags, and the tag handled in
 *   functions called from PackClosure(): PackGeneric, or specialised
 *   ones.
 *
 *   Restoring the tags: Tags must be restored at every place where we
 *   put a reference to the closure. Here: when we fill in the
 *   pointers to a closure. The tags are restored right after
 *   unpacking, inside unpackClosure(). See comments there for details.
 *
 *  About relocatable binaries:
 *   Packing relies on shipping pointers to info tables between
 *   running RTS instances. When these addresses are unstable
 *   (relocatable binaries), they need to be computed using a known
 *   (and usually very low) address is subtracted from any info
 *   pointer packed.
 *
 *   We use the terminology of an "info offset" as opposed to an "info
 *   pointer".  (Furthermore there are proper "info tables", see
 *   ClosureMacros.h for the difference ->TABLES_NEXT_TO_CODE ).
 *
 *   Info offsets are computed from the info pointer in the heap
 *   closure's header when packing, tagged (see above) and put into
 *   the packet. The receiver restores the info pointer and uses the
 *   tag for pointers to the respective unpacked closure.
 *
 *******************************************************************/

// packing a static value
STATIC_INLINE void PackPLC(PackState* p, StgPtr addr) {
    Pack(p, PLC);                     // weight
    // pointer tag of addr still present, packed as-is (with offset)
    Pack(p, (StgWord) P_OFFSET(addr)); // address
}

// packing an offset (repeatedly packed same closure)
STATIC_INLINE void PackOffset(PackState* p, StgWord offset) {
    Pack(p, OFFSET);    // weight
    //  Pack(0L);       // pe
    Pack(p, offset);    // slot/offset
}

// helper accessing the pack buffer
STATIC_INLINE void Pack(PackState* p, StgWord data) {
    ASSERT(p->position < p->size);
    p->buffer[p->position++] = data;
}

// pmtryPackToBuffer: interface function called by the foreign primop.
// Returns packed size (in bytes!) + P_ERRCODEMAX when successful, or
// error codes upon failure
int pmtryPackToBuffer(StgClosure* closure, StgArrWords* mutArr) {
    int errcode = P_SUCCESS; // error code returned by PackClosure
    PackState* p;
    nat size;

    IF_DEBUG(prof, {
            char fpstr[MAX_FINGER_PRINT_LEN];
            graphFingerPrint(fpstr, closure);
            debugBelch("Packing subgraph @ %p\nGraph fingerprint is\n"
                       "\t{%s}\n", closure, fpstr);
        });
    p = initPacking(mutArr);

    queueClosure(p->queue, closure);
    do {
        errcode = packClosure(p, deQueueClosure(p->queue));
        if (errcode != P_SUCCESS) {
            donePacking(p);
            return (errcode);
            // small value => error (real size offset by P_ERRCODEMAX)
        }
    } while (!queueEmpty(p->queue));

    /* Check for buffer overflow (again) */
    ASSERT((p->position + DBG_HEADROOM) < p->size);
    IF_DEBUG(sanity, // write magic end-of-buffer word
             p->buffer[p->position++] = END_OF_BUFFER_MARKER);

    /* Record how much space the graph needs in packet and in heap */
    size = p->position; // need to offset it for the primop to recognise errors
    // globalPackBuffer->unpacked_size = unpacked_size; XXX unpackedSize

    IF_DEBUG(prof,
             debugBelch("** Finished packing graph %p (%s); "
                        "packed size: %d words; size of graph: %d\n",
                        closure, info_type(UNTAG_CLOSURE(closure)),
                        size, 0)); // globalPackBuffer->unpacked_size));

    /* done packing */
    donePacking(p);

    IF_DEBUG(sanity, pmcheckPacket(mutArr->payload, size));

    size = size*sizeof(StgWord) + P_ERRCODEMAX;
    // need offset to recognise errors in primop

    return (int) size;
}


/*
 * @packClosure@ is the heart of the normal packing code.  It packs a
 * single closure into the pack buffer, skipping over any
 * indirections, queues any child pointers for further packing.
 *
 * The routine returns error codes (see Errors.h) indicating error
 * status when packing a closure fails.
 */

StgWord packClosure(PackState* p, StgClosure *closure) {

    StgInfoTable *info;
    StgWord offset;

    // Ensure we can always pack this closure as an offset/PLC.
    if (!roomToPack(p, sizeofW(StgWord)))
        return P_NOBUFFER;

loop:
    closure = unwindInd(closure);
    // now closure is the thing we want to pack
    // ... but might still be tagged.

    offset = offsetFor(p, closure);
    // If the closure has been packed already, pack an indirection
    if (offset != 0) {
        PackOffset(p, offset);
        return P_SUCCESS;
    }

    // remove the tag (temporary, subroutines will handle tag as needed)
    info = get_itbl(UNTAG_CLOSURE(closure));

    // code relies on info-pointers being word-aligned (they are tagged)
    ASSERT(info == UNTAG_CAST(StgInfoTable*, info));

    switch (info->type) {

        // follows order of ClosureTypes.h...
    case INVALID_OBJECT:
        barf("Found invalid object");

    case CONSTR:
    case CONSTR_1_0:
    case CONSTR_0_1:
    case CONSTR_2_0:
    case CONSTR_1_1:
    case CONSTR_0_2:
        return PackGeneric(p, closure);

    case CONSTR_STATIC:        // We ship indirections to CAFs: They are
    case CONSTR_NOCAF_STATIC:  // evaluated on each PE if needed
    case FUN_STATIC:
    case THUNK_STATIC:
        // all these are packed with their tag (closure is still tagged)
        IF_DEBUG(sparks,
                 debugBelch("*>~~ Packing a %p (%s) as a PLC\n",
                            closure, info_type_by_ip(info)));

        PackPLC(p, (StgPtr)closure);
        // NB: unpacked_size of a PLC is 0
        return P_SUCCESS;

    case FUN:
    case FUN_1_0:
    case FUN_0_1:
    case FUN_2_0:
    case FUN_1_1:
    case FUN_0_2:
        return PackGeneric(p, closure);

    case THUNK:
    case THUNK_1_0:
    case THUNK_0_1:
    case THUNK_2_0:
    case THUNK_1_1:
    case THUNK_0_2:
        // !different layout! (smp update field, see Closures.h)
        // the update field should better not be shipped...
        return PackGeneric(p, closure);

    case THUNK_SELECTOR:
        // a thunk selector extracts one of the arguments of another
        // closure. See GC.c::eval_thunk_selector. Selectee might be
        // CONSTR*, or IND* or unevaluated (THUNK*, AP, AP_STACK,
        // BLACKHOLE).

        // GC tries to evaluate and eliminate THUNK_SELECTORS by
        // following them. unwindInd could include them in the normal
        // case, but this is fatal in case of a loop. Therefore, just
        // pack selector and selectee instead. getClosureInfo treats
        // the selectee in this closure type as a pointer field.

        return PackGeneric(p, closure);

    case BCO:
        goto unsupported;

    case AP:
    case PAP:
        return PackPAP(p, (StgPAP *)closure); // types with bitmap-layout

    case AP_STACK:
        // this is a stack from an evaluation that was interrupted
        // (by an exception or alike). Slightly unclear whether it
        // would ever make sense to pack/replicate it.
        goto unsupported;

    case IND:
    case IND_PERM:
    case IND_STATIC:
        // clearly a bug!
        barf("Pack: found IND_... after shorting out indirections %d (%s)",
             (nat)(info->type), info_type_by_ip(info));

        // return vectors
    case RET_BCO:
    case RET_SMALL:
    case RET_BIG:
    case RET_FUN:
        goto impossible;

        // stack frames
    case UPDATE_FRAME:
    case CATCH_FRAME:
    case UNDERFLOW_FRAME:
    case STOP_FRAME:
        goto impossible;

    case BLOCKING_QUEUE:
        goto impossible;

    case BLACKHOLE:
        //  case RBH:
        {
            StgClosure* indirectee = ((StgInd*)closure)->indirectee;

            // some Blackholes are actually indirections since ghc-7.0
            switch (((StgInfoTable*)get_itbl(UNTAG_CLOSURE(indirectee)))->type) {

            case IND: // race cond. when threaded (blackhole just got updated)
                // This case is analogous with the one in StgMiscClosures.cmm
                goto loop;
            case TSO: // blackhole without blocking queue
            case BLOCKING_QUEUE: // another thread already blocked here

                // NB If desired and the calling TSO is known, it can block
                // on this Blackhole until it is updated/data arrives.
                // The TSO should then restart packing when woken up.

                IF_DEBUG(sparks,
                         debugBelch("packing hit a %s at %p (returning).\n",
                                    info_type_by_ip(info), closure));
                return P_BLACKHOLE;

            default: // an indirection, pack the indirectee (jump back to start)
                closure = indirectee;
                // race condition, "unwindInd" should have removed this.
                goto loop;
            }
        }

    case MVAR_CLEAN:
    case MVAR_DIRTY:
    case TVAR:
        IF_DEBUG(prof,
                 errorBelch("Pack: packing type %s (%p) not possible",
                            info_type_by_ip(info), closure));
        return P_CANNOTPACK;

    case ARR_WORDS:
        // Word arrays follow the "pointers-first" layout (with no pointers)
        return PackGeneric(p, closure);

    case MUT_ARR_PTRS_CLEAN:
    case MUT_ARR_PTRS_DIRTY:
    case MUT_ARR_PTRS_FROZEN0:
    case MUT_ARR_PTRS_FROZEN:
        // Arrays of pointers have a card table to indicate dirty cells,
        // therefore not the simple pointers/nonpointers layout.
        // NB At this level, we cannot distinguish immutable arrays
        // from mutable ones
        return PackArray(p, closure);

    case MUT_VAR_CLEAN:
    case MUT_VAR_DIRTY: // known as IORefs in the Haskell world
        IF_DEBUG(prof,
                 errorBelch("Pack: packing type %s (%p) not possible",
                            info_type_by_ip(info),closure));
        return P_CANNOTPACK;

    case WEAK:
        goto unsupported;

    case PRIM:
        // Prim type holds internal immutable closures: MSG_TRY_WAKEUP,
        // MSG_THROWTO, MSG_BLACKHOLE, MSG_NULL, MVAR_TSO_QUEUE
    case MUT_PRIM:
        // Mut.Prim type holds internal mutable closures:
        // TVAR_WATCH_Q, ATOMIC_INVARIANT, INVARIANT_CHECK_Q, TREC_HEADER
    case TSO:
        // this might actually happen if the user is smart and brave
        // enough (a thread id in Haskell is a TSO ptr in the RTS)
        goto unsupported;

    case STACK:
    case TREC_CHUNK:  // recorded transaction on STM. Should not occur
        goto impossible;

        // more stack frames:
    case ATOMICALLY_FRAME:
    case CATCH_RETRY_FRAME:
    case CATCH_STM_FRAME:  // STM-related stack frames. Should not occur
        goto impossible;

    case WHITEHOLE:
        // closure is spin-locked, loop back and spin until changed. Take the big
        // round to avoid compiler optimisations getting into the way
        write_barrier();
        goto loop;
        // valid only for the threaded RTS... cannot distinguish here

#if __GLASGOW_HASKELL__ > 708
    case SMALL_MUT_ARR_PTRS_CLEAN:
    case SMALL_MUT_ARR_PTRS_DIRTY:
    case SMALL_MUT_ARR_PTRS_FROZEN:
    case SMALL_MUT_ARR_PTRS_FROZEN0:
        // unlike the standard arrays, small arrays do not have a card table
        // Layout is thus: +------------------------------+
        //                 | hdr | #ptrs | payload (ptrs) |
        //                 +------------------------------+
        // No problem with using PackGeneric and vhs=1 in getClosureInfo
        return PackGeneric(p, closure);
#endif

unsupported:
        IF_DEBUG(prof,
                 errorBelch("Pack: packing type %s (%p) not implemented",
                            info_type_by_ip(info), closure));
        return P_UNSUPPORTED;

impossible:
        IF_DEBUG(prof,
                 errorBelch("{Pack}Daq Qagh: found %s (%p) when packing",
                            info_type_by_ip(info), closure));
        return P_IMPOSSIBLE;

    default:
        barf("Pack: strange closure %d", (nat)(info->type));
    } // closure type switch

}

// XXX rename packGeneric
// packGeneric packs all closures with "pointers-first" layout
//       +-------------------------------------------------+
//       | FIXED HEADER | VARIABLE HEADER | PTRS | NON-PRS |
//       +-------------------------------------------------+
//  The first (and only, in the default system) word of the header is
//  the info pointer. It is tagged and offset to a known base.
static StgWord PackGeneric(PackState* p, StgClosure* closure)
{
    nat size, ptrs, nonptrs, vhs, i;
    StgWord tag=0;
    StgClosure* infoptr; // actually just a pointer...

    // store tag separately, pack with info ptr
    tag = GET_CLOSURE_TAG(closure);
    closure = UNTAG_CLOSURE(closure);

    // get info about basic layout of the closure
    getClosureInfo(closure, NULL, &size, &ptrs, &nonptrs, &vhs);

    ASSERT(!isBlackhole(closure));

    IF_DEBUG(sparks,
             debugBelch("*>== %p (%s): generic packing"
                        "(size=%d, ptrs=%d, nonptrs=%d, and tag %d)\n",
                        closure, info_type(closure), size, ptrs, nonptrs,
                        (int)tag));

    // make sure we can pack this closure into the current buffer
    if (!roomToPack(p, HEADERSIZE + vhs + nonptrs))
        return P_NOBUFFER;

    // Record that this has been packed
    registerOffset(p, closure);

    // GUM would allocate a GA for the packed closure if it is a thunk
#ifdef GUM
    // Checks for globalisation scheme; default: globalise everything thunks
    if ( RtsFlags.ParFlags.globalising == 0 ||
         (closure_THUNK(closure) && !closure_UNPOINTED(closure)) )
        GlobaliseAndPackGA(closure);
    else
#endif
        Pack(p, (StgWord) CLOSURE);  // marker for unglobalised closure

    // At last! A closure we can actually pack!

    // pack fixed and variable header
    // First word (==infopointer) is tagged and offset using macros
    infoptr = *((StgClosure**) closure);
    Pack(p, (StgWord) (P_OFFSET(TAG_CLOSURE(tag, infoptr))));

    // pack the rest of the header (variable header)
    for (i = 1; i < HEADERSIZE + vhs; ++i) {
        Pack(p, (StgWord)*(((StgPtr)closure)+i));
    }

    // register all ptrs for further packing
    for (i = 0; i < ptrs; ++i) {
        queueClosure(p->queue, ((StgClosure *) *(((StgPtr)closure)+(HEADERSIZE+vhs)+i)));
    }

    // pack non-ptrs
    for (i = 0; i < nonptrs; ++i) {
        Pack(p, (StgWord)*(((StgPtr)closure)+(HEADERSIZE+vhs)+ptrs+i));
    }

    ASSERT(HEADERSIZE+vhs+ptrs+nonptrs==size); // no slop in closure, all packed

    // unpacked_size += size; XXX unpacked_size in PackState

#ifdef GUM
    // Record that this is a revertable black hole so that we can fill
    // in its address from the fetch reply.  Problem: unshared thunks
    // may cause space leaks this way, their GAs should be deallocated
    // following an ACK.

    // convert to RBH
    if (closure_THUNK(closure) && !closure_UNPOINTED(closure)) {
        StgClosure *rbh;
        rbh = convertToRBH(closure);
        ASSERT(size>=HEADERSIZE+MIN_UPD_SIZE); // min size for updatable closure
        ASSERT(rbh == closure);         // rbh at same position (minced version)

        // record the thunk that has been packed so that we may abort and revert
        if (thunks_packed < MAX_THUNKS_PER_PACKET)
            thunks[thunks_packed++] = closure;
        // otherwise: abort packing right now (should not happen at all).
    }
#endif

    return P_SUCCESS;
}

// Packing PAPs and APs:

// a PAP (partial application) represents a function which has been
// given too few arguments for complete evaluation (thereby defining a
// new function with fewer arguments).
//
// PAP/AP closure layout in GHC (see Closures.h, InfoTables.h):
//   +--------------------------------------------------------------+
//   | Header | (arity | n_args) | Function | Stack.|Stack.|Stack...|
//   +--------------------------------------------------------------+
//                                     |
//                   (info table has bitmap for stack)
//
// The _arity_ of the PAP informs about how many arguments are still
// missing to saturate the function call. n_args, in turn, is how many
// arguments are already present (i.e. the stack size).
//
// An APs (generic application) has similar layout, but actually has
// all its arguments, i.e. the application has not been evaluated to
// WNHF yet. Therefore, APs have a thunk header (one extra word).
//
//  PAPs/APs are packed by packing the function and the argument stack,
// where both can point to static or dynamic (heap-allocated) closures
// which must be packed later, and enqueued here.
// The stack may contain either pointers or non-pointer words, indicated
// by a _bitmap_ that comes with the function (but is only used up to the
// indicated n_args size).
// Old code used tags on all stored values (doubling the stack size),
// this version packs the btimap instead.
static StgWord PackPAP(PackState *p, StgPAP *pap) {

    nat i;
    nat hsize;          // header size
    StgWord bitmap;     // small bitmap
    StgLargeBitmap *lbm;// large bitmap
    nat bsize;          // bitmap size
    nat bsizeW;         // bitmap size in words
    StgFunInfoTable *funInfo; // to get bitmap

    nat n_args;         // arg. count on stack
    StgClosure *fun;    // function in PAP/AP
    StgPtr ptr;         // stack object currently packed
    StgWord tag = 0;

    tag = GET_CLOSURE_TAG((StgClosure*) pap);
    pap = UNTAG_CAST(StgPAP*, (StgClosure*) pap);

    ASSERT(LOOKS_LIKE_CLOSURE_PTR(pap));
    ASSERT(get_itbl((StgClosure*)pap)->type == PAP ||
            get_itbl((StgClosure*)pap)->type == AP);

    switch (get_itbl((StgClosure*)pap)->type) {
    case PAP:
        n_args = pap->n_args;
        hsize  = HEADERSIZE+1;
        fun    = pap->fun;
        break;

    case AP:
            n_args = ((StgAP*) pap)->n_args;
            hsize  = sizeofW(StgThunkHeader)+1;
            fun    = ((StgAP*) pap)->fun;
            break;

    default: // checked in packClosure, should not happen here
        barf("PackPAP: strange info pointer, type %d ",
             get_itbl((StgClosure*)pap)->type);
    }

    IF_DEBUG(sparks, {
            debugBelch("Packing %s closure @ %p,"
                       "with stack of size %d\n",
                       info_type((StgClosure*) pap), pap, n_args);
        });

    // Extract the bitmap from the function.
    // Bitmaps can be either small (1 StgWord) or large
    // (StgLargeBitmap, see InfoTables.h) with a size field and
    // multiple bitmap fields.
    // Note that only the bits up to n_args are used in the packing code,
    // therefore the packed bitmap is not necessarily the complete one.
    //
    // small bitmap:
    // (32 bit StgWord) [ bits 5-31: bitmap | bits 0-4: size ]
    // (64 bit StgWord) [ bits 5-63: bitmap | bits 0-5: size ]
    //                          <--reading--|
    //
    // note that reading direction for bitmaps is right-to-left per
    // StgWord (but left-to-right in the large for large bitmaps)
    // see rts/sm/Scav.c::scavenge_(small|large)_bitmap

    lbm = (StgLargeBitmap*) NULL;
    funInfo = get_fun_itbl(UNTAG_CLOSURE(fun));
    switch (funInfo->f.fun_type) {

        // these two use a large bitmap.
        case ARG_GEN_BIG:
            errorBelch("PackPAP at %p: large bitmap not implemented",
                       pap);
            return P_UNSUPPORTED;
            // lbm set indicates a large bitmap (bad if all non-pointers! :-)
            lbm   = GET_FUN_LARGE_BITMAP(funInfo);
            bsizeW = lbm->size / BITS_IN(StgWord);
            break;
        case ARG_BCO:
            errorBelch("PackPAP at %p: large bitmap not implemented",
                       pap);
            return P_UNSUPPORTED;
            // lbm indicates large bitmap. BCO macro needs fun ptr, not info
            lbm   = BCO_BITMAP(fun);
            bsizeW = lbm->size / BITS_IN(StgWord);
            break;

        // another clever solution: fields in info table different for
        // some cases... and referring to autogenerated constants (Apply.h)
        case ARG_GEN:
            bitmap = funInfo->f.b.bitmap;
            bsizeW  = 1;
            break;

        default:
            bitmap = stg_arg_bitmaps[funInfo->f.fun_type];
            bsizeW  = 1;
    }


    // check that we have enough space... upper bound on required size:
    //         header + arg.s (all non-ptrs) + bitmap and its tag
    if (!roomToPack(p, hsize + n_args + 1 + bsizeW))
        return P_NOBUFFER;

    // XXX unpacked_size += hsize + 1 + n_args; // == closure_size(pap)

    // register closure
    registerOffset(p, (StgClosure*) pap);

    // do the actual packing!
    // PAP layout in pack buffer
    //   +---------------------------....................---------------------+
    //   | Header | (arity | n_args) | bsizeW | bitmap.. | nonPtr | nonPtr|...|
    //   +---------------------------....................---------------------+
    // Function field and pointers on stack are not packed but enqueued. In
    // turn, the packet contains the bitmap, together with its size (or value
    // 0xFF..FF to tag a small bitmap)

    // pack closure marker
    Pack(p, (StgWord) CLOSURE);

    // pack header. First word (infoptr) is tagged and offset
    Pack(p, (StgWord) (P_OFFSET(TAG_CLOSURE(tag, (StgClosure*) *((StgPtr) pap
                                                                 )))));
    // rest of header packed as-is (possibly padding, then arity|n_args)
    for(i = 1; i < hsize; i++) {
        Pack(p, (StgWord) *(((StgWord*)pap)+i));
    }

    // queue the function closure for later packing
    queueClosure(p->queue, fun);

    // pack the bitmap
    // the bitmap is preceded by a tag, SMALL_BITMAP_TAG == ~0L for a small one,
    // its size in bits for a large bitmap.

    // Then we pack the bitmap itself.
    // Note that packing only n_args/BITS_IN(StgWord) bits would do, only those
    // bits are actually used in the packing/unpacking code). However, we do not
    // save much, and the case is very rare anyway.
    if ( lbm == NULL ) {
        // small bitmap, tag and pack it
        // SMALL_BITMAP_TAG is ~0L, a very unlikely size
        Pack(p, SMALL_BITMAP_TAG);
        Pack(p, bitmap);
    } else {
        // large bitmap, a lot of gymnastics
        IF_DEBUG(sparks,
                 debugBelch("yuck, large bitmap"));
        return P_UNSUPPORTED; // XXX following code is an unchecked draft
        // use size as tag for large bitmap (see above, ~0L is unlikely size)
        Pack(p, bsizeW);
        // for (i=0; i * BITS_IN(StgWord) < n_args; i++) { // meeh, we pack all
        for (i=0; i < bsizeW; i++) {
            Pack(p, lbm->bitmap[i]);
        }
    }

    // now walk the stack, packing non-pointers and enqueueing pointers, as
    // indicated by bitmap, see Scav.c::scavenge_(small|large)_bitmap (which
    // only evacuates pointers)

    // ptr = first word of payload (PAP/AP cases separated above)
    if (lbm == NULL) {
        bsize = BITMAP_SIZE(bitmap);
        bitmap = BITMAP_BITS(bitmap);
        while (bsize > 0) {
            if (bitmap & 1) {
                // bit set => non-pointer, pack
                Pack(p, *ptr);
            } else {
                // bit not set => pointer
                queueClosure(p->queue, (StgClosure*) *ptr);
                // XXX unpacked_size += sizeofW(StgInd); // unpacking creates IND
            }
            ptr++;
            bitmap = bitmap >> 1;
            bsize--;
        }
    } else {
        debugBelch("yuck, large bitmap again");
        return P_UNSUPPORTED;
        // XXX following code UNCHECKED!
        // written to closely match Scav.c::scavenge_large_bitmap
        nat j, b;
        b = 0;
        bsize = lbm->size;
        for(i = 0; i < bsize; b++) {
            bitmap = lbm->bitmap[b];
            j = stg_min(bsize-i, BITS_IN(StgWord));
            i += j;
            for (; j > 0; j--, ptr++) {
                if (bitmap & 1) { // bit set => non-pointer
                    Pack(p, *ptr);
                } else { // bit not set => pointer
                    queueClosure(p->queue, (StgClosure*) *ptr);
                    // XXX unpacked_size += sizeofW(StgInd); // unpacking creates IND
                }
                bitmap = bitmap >> 1;
            }
        }
    }

    return P_SUCCESS;
}

// Packing Arrays.

// An Array in the heap can contain StgWords or Pointers (to
// closures), and is thus of type StgArrWords or StgMutArrPtrs.
//
//     Array layout in heap/buffer is the following:
//
//  (packed into the buffer)
// +------------------------+......................................+
// | IP'| Hdr | ptrs | size | ptr1 | ptr2 | .. | ptrN | card space |
// +------------------------+......................................+
//                               (added in heap when unpacking)
//
// The array size is stored in bytes, but will always be word-aligned.
//
// Historically, this routine was also packing ArrWords, but they can
// equally well be treated as "pointers-first" generic layout (with no
// pointers), and are packed simply by copying all words (as non-ptrs).
//
// MutArrPtrs (MUT_ARRAY_PTRS_* types) contain pointers to other
// closures instead of words.
// Packing MutArrPtrs means to enqueue/pack all pointers found.
// OTOH, packing=copying a mutable array is not a good idea at all.
// We implement it even though, leave it to higher levels to restrict.
static StgWord PackArray(PackState *p, StgClosure *closure) {

    StgInfoTable *info;
    StgClosure *infoptr;
    nat i, payloadsize, packsize;

    /* remove tag, store it in infopointer (same as above) */
    StgWord tag=0;

    tag = GET_CLOSURE_TAG(closure);
    closure = UNTAG_CLOSURE(closure);

    /* get info about basic layout of the closure */
    info = get_itbl(closure);

    ASSERT( info->type == MUT_ARR_PTRS_CLEAN
            || info->type == MUT_ARR_PTRS_DIRTY
            || info->type == MUT_ARR_PTRS_FROZEN0
            || info->type == MUT_ARR_PTRS_FROZEN);

    // MUT_ARR_PTRS_* {HDR,(no. of)ptrs,size(total incl.card table)}
    // Only pack header, not card table which follows the data.
    packsize = HEADERSIZE + 2;
    payloadsize = ((StgMutArrPtrs *)closure)->ptrs;

    // the function in ClosureMacros.h would include the header:
    // arr_words_sizeW(stgCast(StgArrWords*,q));
    IF_DEBUG(sparks,
             debugBelch("*>== %p (%s): packing array"
                        "(%d words) (size=%d)\n",
                        closure, info_type(closure), payloadsize,
                        (int)closure_sizeW(closure)));

    // check if enough room in the pack buffer
    if (!roomToPack(p, packsize)) return P_NOBUFFER;

    // record offset of the closure */
    registerOffset(p, closure);

    Pack(p, (StgWord) CLOSURE);  // marker for unglobalised closure (array)

    // Pack the header and the number of bytes/ptrs that follow)
    // First word (info pointer) is tagged and offset
    infoptr = *((StgClosure**) closure);
    Pack(p, (StgWord) (P_OFFSET(TAG_CLOSURE(tag, infoptr))));

    // pack the rest of the header (variable header)
    for (i = 1; i < HEADERSIZE; ++i)
        Pack(p, (StgWord)*(((StgPtr)closure)+i));

    // pack no. of ptrs and total size, enqueue pointers
    Pack(p, (StgWord) ((StgMutArrPtrs *)closure)->ptrs);
    Pack(p, (StgWord) ((StgMutArrPtrs*)closure)->size);
    for (i=0; i<payloadsize; i++)
        queueClosure(p->queue, ((StgMutArrPtrs *) closure)->payload[i]);

    // unpacked_size += closure_sizeW(closure); XXX unpacked_size

    return P_SUCCESS;
}

/*******************************************************************
 *   unpacking a graph structure:
 *******************************************************************/

/*
  @UnpackGraph@ unpacks the graph contained in a message buffer.  It
  returns a pointer to the new graph.

  UnpackGraph uses a number of global static fields. Therefore, the lock
  pack_mutex should be held when calling it (can be released immediately
  after).

  The inPort parameter restores the unpack state when the sent graph
  is sent in more than one message.

  Formerly, we also had a globalAddr** @gamap@ parameter: set to
  point to an array of (oldGA,newGA) pairs which were created as a result
  of unpacking the buffer; and nat* @nGAs@ set to the number of GA pairs which
  were created.

  With the new per-capability allocation, we need to pass the cap.
  parameter around all the time. Could put it into a global unpacking
  state...

  for "pointer tagging", we assume here that all stored
  info pointers (each first word of a packed closure) also carry the
  tag found at the sender side when enqueueing it (for the first
  time!). When closures are unpacked, the tag must be added before
  inserting the result of unpacking into other closures as a pointer.
  Done by UnpackClosure(), see there.
*/

// unpacking from a Haskell array (using the Haskell Byte Array)
// may return error code P_GARBLED
StgClosure* pmUnpackGraphWrapper(StgArrWords* packBufferArray, Capability* cap)
{
    nat size;
    StgWord *buffer;
    StgClosure* newGraph;

    size = packBufferArray->bytes / sizeof(StgWord);
    buffer = (StgWord*) packBufferArray->payload;

    // unpack. Might return NULL in case the buffer was inconsistent.
    newGraph = pmUnpackGraph_(buffer, size, cap);

    return (newGraph == NULL ? (StgClosure *) P_GARBLED : newGraph);
}

// Internal worker function, not allowed to edit the buffer at all
// (used with with an immutable Haskell ByteArray# as buffer for
// deserialisation). This function returns NULL upon
// errors/inconsistencies in buffer (avoiding to abort the program).
StgClosure* pmUnpackGraph_(StgWord *buffer, StgInt size, Capability* cap) {
                        // replace these two by a struct
    StgWord* bufptr;
    StgClosure *closure, *parent, *graphroot;
    nat pptr = 0, pptrs = 0, pvhs = 0;
    nat currentOffset;
    HashTable* offsets;
    ClosureQ* queue;

    IF_DEBUG(prof,
             debugBelch("Unpacking buffer @ %p (%" FMT_Word " words)\n",
                        buffer, size));
    IF_DEBUG(sanity, pmcheckPacket(buffer, size));

    offsets = allocHashTable();
    queue   = initClosureQ(size / 2);

    graphroot = parent = (StgClosure *) NULL;
    bufptr = buffer;

    do {
        // check that we aren't at the end of the buffer, yet
        IF_DEBUG(sanity, ASSERT(*bufptr != END_OF_BUFFER_MARKER));

        // Compute the offset to register for future back references
        // If this is itself an offset, or a PLC, we do not store anything
        if (*bufptr == OFFSET || *bufptr == PLC) {
            currentOffset = 0;
        } else {
            currentOffset = ((nat) (bufptr - buffer)) + PADDING;
            // ...which is at least 1 (PADDING)
        }

        // Unpack one closure (or offset or PLC). This allocates heap
        // space, checks for PLC/offset etc. The returned pointer is
        // tagged with the tag found in the info pointer.
        closure = UnpackClosure (queue, offsets, &bufptr, cap);

        if (closure == NULL) {
            // something is wrong with the packet, give up immediately
            // we do not try to find out details of what is wrong...
            IF_DEBUG(prof, debugBelch("Unpacking error at address %p",bufptr));
            freeHashTable(offsets, NULL);
            freeClosureQ(queue);
            return (StgClosure *) NULL;
        }

        // store closure address for offsets (if we should, see above)
        if (currentOffset != 0) {
            IF_DEBUG(sparks,
                     debugBelch("---> Entry in Offset Table: (%d, %p)\n",
                                currentOffset, closure));
            // note that the offset is stored WITH TAG
            insertHashTable(offsets, currentOffset, (void*) closure);
        }

        // Set the pointer in the parent to point to chosen
        // closure. If we're at the top of the graph (our parent is
        // NULL), then we want to return this closure to our caller.
        if (parent == NULL) {
            /* we are at the root. Do not remove the tag */
            graphroot = closure;
            IF_DEBUG(prof, debugBelch("Graph root %p, tag %x", closure,
                                      (int) GET_CLOSURE_TAG(closure)));
        } else {
            // packet fragmentation code would need to check whether
            // there is a temporary blackhole here. Not supported.

            // write ptr to new closure into parent at current position (pptr)
            ((StgPtr) parent)[HEADERSIZE + pvhs + pptr] = (StgWord) closure;
        }

        // Locate next parent pointer (incr ppr, dequeue next closure at end)
        locateNextParent(queue, &parent, &pptr, &pptrs, &pvhs);

        // stop when buffer size has been reached or end of graph
    } while ((parent != NULL) && (size > (bufptr-buffer)));

    if (parent != NULL) {
        // this case is valid when one graph can stretch across
        // several packets (fragmentation), in which case we would
        // save the state. Not supported here.

        IF_DEBUG(prof, errorBelch("Pack buffer overrun"));
        return (StgClosure *) NULL;
    }

    freeHashTable(offsets, NULL);
    freeClosureQ(queue);

    // check magic end-of-buffer word
    IF_DEBUG(sanity, ASSERT(*(bufptr++) == END_OF_BUFFER_MARKER));

    // assert we unpacked exactly as many words as there are in the buffer
    ASSERT(size == (nat) (bufptr-buffer));

    // ToDo: are we *certain* graphroot has been set??? WDP 95/07
    ASSERT(graphroot!=NULL);

    IF_DEBUG(prof, {
            char fpstr[MAX_FINGER_PRINT_LEN];
            graphFingerPrint(fpstr, graphroot);
            debugBelch(">>> unpacked graph at %p\n Fingerprint is\n"
                       "\t{%s}\n", graphroot, fpstr);
                });

    return graphroot;
}

// locateNextParent finds the next pointer field in the parent
// closure, retrieve information about its variable header size and
// no. of pointers. If the current parent has been completely unpacked
// already, get the next closure from the global closure queue, and
// register the new variable header size and no. of pointers.
//
// Example situation:
//
// *parentP
//    |
//    V
//  +--------------------------------------------------------------------+
//  |hdr| variable hdr  | ptr1 | ptr2 | ptr3 | ... | ptrN | non-pointers |
//  +--------------------------------------------------------------------+
//      <-- *pvhsP=2 --->                A
//                                       |
//         *pptrs = N                 *pptr=3
STATIC_INLINE void locateNextParent(ClosureQ* q, StgClosure** parentP,
                                    nat* pptrP, nat* pptrsP, nat* pvhsP) {
    nat size, nonptrs;

    // pptr as an index into the current parent; find the next pointer
    // field in the parent by increasing pptr; if that takes us off
    // the closure (i.e. *pptr + 1 > *pptrs) grab a new parent from
    // the closure queue

    (*pptrP)++;
    while (*pptrP + 1 > *pptrsP) {
        // *parentP has been constructed (all pointer set); so check it now
        IF_DEBUG(sanity,
                if (*parentP != (StgClosure*)NULL) // not root
                checkClosure(*parentP));

        *parentP = deQueueClosure(q);

        if (*parentP == NULL) {
            break;
        } else {
            getClosureInfo(*parentP, NULL, &size, pptrsP, &nonptrs, pvhsP);
            *pptrP = 0;
        }
    }
    // *parentP points to the new (or old) parent;
    // *pptr, *vhsP, and *pptrs have been updated referring to the new parent
}

//  UnpackClosure is the heart of the unpacking routine. It is called for
//  every closure found in the packBuffer.
//  UnpackClosure does the following:
//    - check for the kind of the closure (PLC, Offset, std closure)
//    - copy the contents of the closure from the buffer into the heap
//    - update LAGA tables (in particular if we end up with 2 closures
//      having the same GA, we make one an indirection to the other)
//    - set the GAGA map in order to send back an ACK message
// In case of any unexpected data, the routine returns NULL.
//
//  At the end of this function,
//  *bufptrP points to the next word in the pack buffer to be unpacked.
//
//  "pointer tagging":
// When unpacking, UnpackClosure() we add the tag to its return value,
// but enqueue the closure address WITHOUT A TAG, so we can access the
// unpacked closure directly by the enqueued pointer.
// The closure WITH TAG is saved as offset value in the offset hash
// table (key=offset, value=address WITH TAG), to be filled in other
// closures as a pointer field.
// When packing, we did the reverse: saved the closure address WITH TAG
// in the queue, but stored it WITHOUT TAG in the offset table (as a
// key, value was the offset).
static  StgClosure*
UnpackClosure (ClosureQ* q, HashTable* offsets,
               StgWord **bufptrP, Capability* cap) {
    StgClosure *closure;
    nat size,ptrs,nonptrs,vhs,i;
    StgInfoTable *ip;
    StgWord tag = 0;

    // Unpack the closure body, if there is one; three cases:
    //   - PLC: closure is just a pointer to a static closure
    //   - Offset: closure has been unpacked already
    //   - else: copy data from packet into closure
    switch ((StgWord) **bufptrP) {
        // these two cases respect the "pointer tags" by
        // design: either the tag was not removed at all (PLC case), or
        // the offset refers to an already unpacked (=> tagged) closure.
        case PLC:
            closure = UnpackPLC(bufptrP);
            break;
        case OFFSET:
            closure = UnpackOffset(offsets, bufptrP);
            break;
        case CLOSURE:

            (*bufptrP)++; // skip marker

            /* The first word of a closure is the info pointer. In contrast,
               in the packet (where (*bufptrP) points to a packed closure),
               the first word is an info _offset_, which additionally was
               tagged before packing. We remove and store the tag added to the
               info offset, and compute the untagged info table pointer from
               the info offset.
               (NB The original value must be untouched inside the buffer!)
            */
            tag = GET_CLOSURE_TAG((StgClosure*) **bufptrP);
            ip  = UNTAG_CAST(StgInfoTable*, P_POINTER(**bufptrP));
            IF_DEBUG(sparks,
                     debugBelch("pointer tagging: removed tag %d "
                                "from info pointer %p in packet\n",
                                (int) tag, ip));

            // The essential part starts here: allocate heap, fill in
            // closure, queue it to fill pointer payload later.
            if (!LOOKS_LIKE_INFO_PTR((StgWord) ip)) {
                errorBelch("Invalid info pointer in packet");
                return (StgClosure *) NULL;
            }

            /* Historic comment:
             * Close your eyes.  You don't want to see where we're
             * looking. You can't get closure info until you've unpacked the
             * variable header, but you don't know how big it is until you've
             * got closure info.  So...we trust that the closure in the buffer
             * is organized the same way as they will be in the heap...at
             * least up through the end of the variable header.
             */

            getClosureInfo((StgClosure *) *bufptrP, INFO_PTR_TO_STRUCT(ip),
                           &size, &ptrs, &nonptrs, &vhs);

            switch (INFO_PTR_TO_STRUCT(ip)->type) {
                // bitmap layouts branch into special routines:
            case PAP:
            case AP:
                closure = UnpackPAP(q, ip, bufptrP, cap);
                // creates/enQs INDirections for pointers on stack
                break;

                // MUT_ARR* need to allocate (but not fill) card table
                // space after data space, and enqueue the closure
            case MUT_ARR_PTRS_CLEAN:
            case MUT_ARR_PTRS_DIRTY:
            case MUT_ARR_PTRS_FROZEN0:
            case MUT_ARR_PTRS_FROZEN:
                closure = UnpackArray(q, ip, bufptrP, cap);
                break;

                // word arrays follow the "ptrs first" layout (with no pointers)
            case ARR_WORDS:

                // normal closures with pointers-first layout (these are exactly
                // the closures handled by PackGeneric above):
            case CONSTR:
            case CONSTR_1_0:
            case CONSTR_0_1:
            case CONSTR_2_0:
            case CONSTR_1_1:
            case CONSTR_0_2:
            case FUN:
            case FUN_1_0:
            case FUN_0_1:
            case FUN_2_0:
            case FUN_1_1:
            case FUN_0_2:
            case THUNK:
            case THUNK_1_0:
            case THUNK_0_1:
            case THUNK_2_0:
            case THUNK_1_1:
            case THUNK_0_2:
            case THUNK_SELECTOR:
#if __GLASGOW_HASKELL__ > 708
            case SMALL_MUT_ARR_PTRS_CLEAN:
            case SMALL_MUT_ARR_PTRS_DIRTY:
            case SMALL_MUT_ARR_PTRS_FROZEN0:
            case SMALL_MUT_ARR_PTRS_FROZEN:
#endif

                IF_DEBUG(sparks,
                    debugBelch("Allocating %d heap words for %s-closure:\n"
                               "(%d ptrs, %d non-ptrs, vhs = %d)\n"
                               , size, info_type_by_ip(INFO_PTR_TO_STRUCT(ip)),
                               ptrs, nonptrs, vhs));

                closure = (StgClosure*) allocate(cap, size);

                // Remember, the generic closure layout is as follows:
                //     +------------------------------------------------+
                //     | IP | FIXED HDR | VARIABLE HDR | PTRS | NON-PRS |
                //     +------------------------------------------------+
                //     Note that info ptr (IP) is assumed to be first hdr. field

                // Fill in the info pointer (extracted before)
                ((StgPtr)closure)[0] = (StgWord) ip;
                (*bufptrP)++;

                // Fill in the rest of the fixed header (if any)
                for (i = 1; i < HEADERSIZE; i++)
                    ((StgPtr)closure)[i] = *(*bufptrP)++;

                // Fill in the packed variable header
                for (i = 0; i < vhs; i++)
                    ((StgPtr)closure)[HEADERSIZE + i] = *(*bufptrP)++;

                // Pointers will be filled in later, but set zero here to
                // easily check if there is a temporary BH.
                for (i = 0; i < ptrs; i++)
                    ((StgPtr)closure)[HEADERSIZE + vhs + i] = 0;

                // Fill in the packed non-pointers
                for (i = 0; i < nonptrs; i++)
                    ((StgPtr)closure)[HEADERSIZE + i + vhs + ptrs]
                        =  *(*bufptrP)++;

                ASSERT(HEADERSIZE+vhs+ptrs+nonptrs == size);

                queueClosure(q, closure);
                break;

                // other cases are unsupported/unexpected, and caught here
            default:
                errorBelch("Unpacking unexpected closure type (%x)\n",
                           INFO_PTR_TO_STRUCT(ip)->type);
                return (StgClosure *) NULL;
            } // switch(INFO_PTR_TO_STRUCT(ip)->type)
            break;

    default:
        // invalid markers (not OFFSET, PLC, CLOSURE) are caught here
        errorBelch("unpackClosure: Found invalid marker %" FMT_Word ".\n",
                   (long) **bufptrP);
        return (StgClosure *) NULL;
    }

    return TAG_CLOSURE(tag, closure);
}

// look up the closure's address for an offset in the hashtable
// advance buffer pointer while reading data
STATIC_INLINE StgClosure *UnpackOffset(HashTable* offsets, StgWord **bufptrP) {
    StgClosure* existing;
    int offset;

    ASSERT((long) **bufptrP == OFFSET);

    (*bufptrP)++; // skip marker
    // unpack nat; find closure for this offset
    offset = (nat) **bufptrP;
    (*bufptrP)++; // skip offset

    ASSERT(offset != 0);
    // find this closure in an offset hashtable (we can have several packets)
    existing = (StgClosure *) lookupHashTable(offsets, offset);

    IF_DEBUG(sparks,
             debugBelch("*<__ Unpacked indirection to closure %p"
                        " (was OFFSET %d)", existing, offset));

    // we should have found something...
    ASSERT(existing!= NULL);

    return existing;
}

// unpack a static address (advancing buffer pointer while reading)
STATIC_INLINE  StgClosure *UnpackPLC(StgWord **bufptrP) {
    StgClosure* plc;

    ASSERT((long) **bufptrP == PLC);

    (*bufptrP)++; // skip marker
    // Not much to unpack; just a static local address
    // but need to correct the offset
    plc = (StgClosure*) P_POINTER(**bufptrP);
    (*bufptrP)++; // skip address
    IF_DEBUG(sparks,
            debugBelch("*<^^ Unpacked PLC at %p\n", plc));
    return plc;
}

// unpacking PAPs (and other bitmap layout). Returns NULL in case of errors
static StgClosure * UnpackPAP(ClosureQ *queue, StgInfoTable *info,
                              StgWord **bufptrP, Capability* cap) {

    nat n_args, size, bsize, bsizeW, hsize, i;
    StgPtr pap; // PAP/AP is constructed here, but untyped (would need
                // to distinguish the AP case all the time)

    // PAP layout in pack buffer
    //   +---------------------------....................---------------------+
    //   | Header | (arity | n_args) | bsizeW | bitmap.. | nonPtr | nonPtr|...|
    //   +---------------------------....................---------------------+
    //
    // The bitmap indicating pointers on the stack is packed after the header.
    // For large bitmaps, their size in words is stored in the buffer; small
    // bitmaps are indicated by a size of 0xFF..FF (SMALL_BITMAP_TAG).
    //
    // In the heap, there will be a function field instead of this bitmap, and
    // the payload (stack) will have pointers interspersed with the packed
    // nonptrs.
    // The function field is filled in the usual way (as if a PAP was
    // "ptrs-first"), the stack will be constructed with pointers to new
    // indirections filled later.

    // Unpacking should result in the following layout in the heap:
    // +----------------------------------------------------------------+
    // | Header | (arity , n_args) | Fct. | Arg/&Ind1 | Arg/&Ind2 | ... |
    // +----------------------------------------------------------------+
    // followed by <= n_args indirections pointed at from the stack

    // calc./alloc. needed closure space in the heap, using common macros.
    switch (INFO_PTR_TO_STRUCT(info)->type) {
    case PAP:
        hsize = HEADERSIZE + 1;
        n_args  = ((StgPAP*) *bufptrP)->n_args;
        size  = PAP_sizeW(n_args);
        break;
    case AP:
        hsize = sizeofW(StgThunkHeader) + 1;
        n_args  = ((StgAP*)  *bufptrP)->n_args;
        size  = AP_sizeW(n_args);
        break;
    default:
        IF_DEBUG(prof,
                 errorBelch("UnpackPAP: strange info pointer, type %d ",
                            INFO_PTR_TO_STRUCT(info)->type));
        return (StgClosure*) NULL;
    }
    IF_DEBUG(sparks,
            debugBelch("allocating %d heap words for a PAP(%d args)\n",
                size, n_args));
    pap = (StgPtr) allocate(cap, size);

    // fill in info ptr (extracted and given as argument by caller)
    pap[0] = (StgWord) info;
    (*bufptrP)++;

    // fill in header fields (includes ( arity | n_args ) )
    for(i = 1; i < hsize; i++) {
        pap[i] = (StgWord) *(*bufptrP)++;
    }
    // enqueue to get function field filled (see getClosureInfo)
    queueClosure(queue, (StgClosure*) pap);
    // zero the function field
    pap[hsize] = (StgWord) NULL;

    // read bitmap size and bitmap
    bsizeW = (nat) *(*bufptrP)++;

    // unpack the stack (size == args), starting at pap[hsize]
    // make room for fct. pointer, thus start at hsize+1

    if (bsizeW == SMALL_BITMAP_TAG) {
        StgWord bitmap;
        // small bitmap, just read it and unpack accordingly
        bitmap = (StgWord)  *(*bufptrP)++;

        // bitmap size irrelevant here, but should be >= n_args
        ASSERT(n_args <= BITMAP_SIZE(bitmap));
        bitmap = BITMAP_BITS(bitmap);
        for (i = hsize + 1; i < hsize + n_args; i++, bitmap >>= 1) {
            if (bitmap & 1) {
                // non-pointer, just unpack it
                pap[i] = (StgWord) *(*bufptrP)++;
            } else {
                // pointer: create and enqueue a new indirection, store a
                // pointer to it on the stack
                StgInd *ind;
                // allocate a new closure
                ind = (StgInd*) allocate(cap, sizeofW(StgInd));
                SET_HDR(ind, &stg_IND_info, CCS_SYSTEM); // set ccs
                // zero the indirectee field (should be filled later)
                ind->indirectee = (StgClosure*) NULL;
                // store a pointer
                pap[i] = (StgWord) ind;
                queueClosure(queue, (StgClosure*) ind);
            }
            bitmap >>= 1;
        }

    } else {
        debugBelch("yuck, unpacking large bitmap");
        return (StgClosure*) NULL;
        // need to repeatedly read a new bitmap and proceed
        StgPtr bitmapPos;
        nat j;
        StgWord bitmap;

        // ... walk through the bitmap until n_args have been unpacked
        bitmapPos = *bufptrP;
        bitmap    = *bitmapPos;
        j = BITS_IN(StgWord);
        for (i = hsize + 1; i < hsize + n_args; i++) {
            if (bitmap & 1) {
                // non-pointer, just unpack it
                pap[i] = (StgWord) *(*bufptrP)++;
            } else {
                // pointer: create and enqueue a new indirection, store a
                // pointer to it on the stack
                StgInd *ind;
                // allocate a new closure
                ind = (StgInd*) allocate(cap, sizeofW(StgInd));
                SET_HDR(ind, &stg_IND_info, CCS_SYSTEM); // set ccs
                // zero the indirectee field (should be filled later)
                ind->indirectee = (StgClosure*) NULL;
                // store a pointer
                pap[i] = (StgWord) ind;
                queueClosure(queue, (StgClosure*) ind);
            }
            // advance into next part of bitmap when current one done
            j--;
            if (j == 0) {
                bitmapPos++;
                bitmap = *bitmapPos;
                j = BITS_IN(StgWord);
            } else {
                bitmap >>= 1;
            }
        }
    }

    return (StgClosure*) pap;
}

// unpacking arrays. Returns NULL in case of errors.
static StgClosure* UnpackArray(ClosureQ *queue, StgInfoTable* info,
                               StgWord **bufptrP, Capability* cap) {
    nat size;
    StgMutArrPtrs *array;

    nat type = INFO_PTR_TO_STRUCT(info)->type;

    // refuse to work if not an array
    if (type != MUT_ARR_PTRS_CLEAN && type != MUT_ARR_PTRS_DIRTY &&
        type != MUT_ARR_PTRS_FROZEN0 && type != MUT_ARR_PTRS_FROZEN) {

        IF_DEBUG(prof,
                 errorBelch("UnpackArray: unexpected closure type %d",
                            INFO_PTR_TO_STRUCT(info)->type));
            return (StgClosure *) NULL;
    }

    // Since GHC-6.13, ptr arrays additionally carry a "card table"
    // for generational GC (to indicate mutable/dirty elements). For
    // unpacking, allocate the card table and fill it with zero.
    // Array layout in buffer:
    // +------------------------+......................................+
    // | IP'| Hdr | ptrs | size | ptr1 | ptr2 | .. | ptrN | card space |
    // +------------------------+......................................+
    //                               (added in heap when unpacking)
    // ptrs indicates how many pointers to come (N). Size field gives
    // total size for pointers and card table behind (to add).

    // size = sizeofW(StgMutArrPtrs) + (StgWord) *((*bufptrP)+2);
    size = closure_sizeW_((StgClosure*) *bufptrP, INFO_PTR_TO_STRUCT(info));
    ASSERT(size == sizeofW(StgMutArrPtrs) + ((StgMutArrPtrs*) *bufptrP)->size);
    IF_DEBUG(sparks,
             debugBelch("Unpacking ptrs array, %" FMT_Word " ptrs, size %d\n",
                        (StgWord) *((*bufptrP)+1), size));
    array = (StgMutArrPtrs *) allocate(cap, size);

    // set area 0 (Blackhole-test in unpacking and card table)
    memset(array, 0, size*sizeof(StgWord));
    // write header
    for (size = 0; size < (sizeof(StgMutArrPtrs)/sizeof(StgWord)); size++)
        ((StgPtr) array)[size] = (StgWord) *(*bufptrP)++;
    // correct first word (info ptr, stored with offset in packet)
    ((StgPtr)array)[0] = (StgWord) info;
    // and enqueue it, pointers will be filled in subsequently
    queueClosure(queue, (StgClosure*)array);

    IF_DEBUG(sparks,
            debugBelch(" Array created @ %p.\n",array));

    return (StgClosure*) array;
}

// debugging functions
#if defined(DEBUG)

/*
  Generate a finger-print for a graph.  A finger-print is a string,
  with each char representing one node; depth-first traversal.
  Will only be called inside this module.
*/

/* this array has to be kept in sync with includes/ClosureTypes.h */
#if __GLASGOW_HASKELL__ == 708
# if !(N_CLOSURE_TYPES == 61 )
# error Wrong closure type count in fingerprint array. Check code.
# endif
#elif __GLASGOW_HASKELL__ > 708
# if !(N_CLOSURE_TYPES == 65 )
# error Wrong closure type count in fingerprint array. Check code.
# endif
#endif
static char* fingerPrintChar =
  "0ccccccCC"    /* INVALID CONSTRs (0-8) */
  "fffffff"      /* FUNs (9-15) */
  "ttttttt"      /* THUNKs (16-23) */
  "TBAPP___"     /* SELECTOR BCO AP PAP AP_STACK INDs (24-31) */
  "RRRRFFFF"     /* RETs FRAMEs (32-39) */
  "*@MMT"        /* BQ BLACKHOLE MVARs TVAR (40-43) */
  "aAAAAmmwppXS" /* ARRAYs MUT_VARs WEAK PRIM MUT_PRIM TSO STACK (44-55) */
  "&FFFW"        /* TREC (STM-)FRAMEs WHITEHOLE (56-60)*/
#if __GLASGOW_HASKELL__ >= 708
  "ZZZZ"         /* SmallArr (61-64) */
#endif
  ;


// recursive worker function:
static void graphFingerPrint_(char* fingerPrintStr,
                              HashTable* tmpClosureTable, StgClosure *p);

static void graphFingerPrint(char* fingerPrintStr, StgClosure *p)
{
    HashTable* visitTable;

    // delete old fingerprint:
    fingerPrintStr[0]='\0';

    /* init hash table */
    visitTable = allocHashTable();

    /* now do the real work */
    graphFingerPrint_(fingerPrintStr, visitTable, p);

    /* nuke hash table */
    freeHashTable(visitTable, NULL);

    ASSERT(strlen(fingerPrintStr)<=MAX_FINGER_PRINT_LEN);
}

/*
  This is the actual worker functions.
  All recursive calls should be made to this function.
*/
static void graphFingerPrint_(char* fp, HashTable* visited, StgClosure *p) {
    nat i, len, args, arity;
    const StgInfoTable *info;
    StgWord *payload;

    // first remove potential pointer tags
    p = UNTAG_CLOSURE(p);

    len = strlen(fp);
    ASSERT(len<=MAX_FINGER_PRINT_LEN);
    if (len+2 >= MAX_FINGER_PRINT_LEN)
        return;
    /* at most 7 chars added immediately (unchecked) for this node */
    if (len+7 >= MAX_FINGER_PRINT_LEN) {
        strcat(fp, "--end");
        return;
    }
    /* check whether we have met this node already to break cycles */
    if (lookupHashTable(visited, (StgWord)p)) { // ie. already touched
        strcat(fp, ".");
        return;
    }

    /* record that we are processing this closure */
    insertHashTable(visited, (StgWord) p, (void *)rtsTrue/*non-NULL*/);

    ASSERT(LOOKS_LIKE_CLOSURE_PTR(p));

    info = get_itbl((StgClosure *)p);

    // append char for this node
    fp[len] = fingerPrintChar[info->type];
    fp[len+1] = '\0';
    /* the rest of this fct recursively traverses the graph */
    switch (info -> type) {

        // simple and static objects
        case CONSTR_STATIC:
        case CONSTR_NOCAF_STATIC:
        case FUN_STATIC:
        case THUNK_STATIC:
            break;

        /* CONSTRs, THUNKs, FUNs are written with arity */
        case THUNK_2_0:
            // append char for this node
            strcat(fp, "20(");
            // special treatment for thunks... extra smp header field
            graphFingerPrint_(fp, visited, ((StgThunk *)p)->payload[0]);
            graphFingerPrint_(fp, visited, ((StgThunk *)p)->payload[1]);
            if (strlen(fp)+2<MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case FUN_2_0:
        case CONSTR_2_0:
            // append char for this node
            strcat(fp, "20(");
            graphFingerPrint_(fp, visited, ((StgClosure *)p)->payload[0]);
            graphFingerPrint_(fp, visited, ((StgClosure *)p)->payload[1]);
            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case THUNK_1_0:
            // append char for this node
            strcat(fp, "10(");
            graphFingerPrint_(fp, visited, ((StgThunk *)p)->payload[0]);
            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case FUN_1_0:
        case CONSTR_1_0:
            // append char for this node
            strcat(fp, "10(");
            graphFingerPrint_(fp, visited, ((StgClosure *)p)->payload[0]);
            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case THUNK_0_1:
        case FUN_0_1:
        case CONSTR_0_1:
            // append char for this node
            strcat(fp, "01");
            break;

        case THUNK_0_2:
        case FUN_0_2:
        case CONSTR_0_2:
            // append char for this node
            strcat(fp, "02");
            break;

        case THUNK_1_1:
            // append char for this node
            strcat(fp, "11(");
            graphFingerPrint_(fp, visited, ((StgThunk *)p)->payload[0]);
            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case FUN_1_1:
        case CONSTR_1_1:
            // append char for this node
            strcat(fp, "11(");
            graphFingerPrint_(fp, visited, ((StgClosure *)p)->payload[0]);
            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fp, ")");
            break;

        case THUNK:
            {
                char str[6];
                sprintf(str,"%d?(", info->layout.payload.ptrs);
                strcat(fp,str);
                for (i = 0; i < info->layout.payload.ptrs; i++)
                    graphFingerPrint_(fp, visited, ((StgThunk *)p)->payload[i]);
                if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fp, ")");
            }
            break;

        case FUN:
        case CONSTR:
            {
                char str[6];
                sprintf(str,"%d?(",info->layout.payload.ptrs);
                strcat(fp,str);
                for (i = 0; i < info->layout.payload.ptrs; i++)
                    graphFingerPrint_(fp, visited,
                                      ((StgClosure *)p)->payload[i]);
                if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fp, ")");
            }
            break;

        case THUNK_SELECTOR:
            graphFingerPrint_(fp, visited, ((StgSelector *)p)->selectee);
            break;

        case BCO:
            break;

        case AP_STACK:
            // unsure how to handle this one
            break;
        case AP:
            arity = ((StgAP*)p)->arity;
            args  = ((StgAP*)p)->n_args;
            payload = (StgPtr)((StgAP*)p)->payload;
            p = ((StgAP*)p)->fun;
            goto print;

        case PAP:
            /* note the arity (total #args) and n_args (how many supplied) */
            arity = ((StgPAP*)p)->arity;
            args  = ((StgPAP*)p)->n_args;
            payload = (StgPtr) ((StgPAP*)p)->payload;
            p = ((StgPAP*)p)->fun;
print:
            { char str[6];
                sprintf(str,"%d/%d(", arity, args);
                strcat(fp, str);
                // follow the function, and everything on the stack
                graphFingerPrint_(fp, visited, (StgClosure *) (p));
                if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN) {
                    StgWord bitmap;
                    StgFunInfoTable *funInfo = get_fun_itbl(UNTAG_CLOSURE(p));
                    strcat(fp, "|");
                    switch (funInfo->f.fun_type) {
                        /* these two use a large bitmap. We do not follow...*/
                        case ARG_GEN_BIG:
                        case ARG_BCO:
                            bitmap = (StgWord) (~0); // all ones
                            break;
                        case ARG_GEN:
                            bitmap = funInfo->f.b.bitmap;
                            break;
                        default:
                            bitmap = stg_arg_bitmaps[funInfo->f.fun_type];
                    }
                    // size = BITMAP_SIZE(bitmap);
                    bitmap = BITMAP_BITS(bitmap);
                    while (args > 0) {
                        if ((bitmap & 1) == 0)
                            graphFingerPrint_(fp, visited,
                                              (StgClosure *)(*payload));
                        else {
                            if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                                strcat(fp, "x");
                        }
                        payload++;
                        args--;
                        bitmap = bitmap>>1;
                    }
                }
                if (strlen(fp)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fp, ")");
            }
            break;

        case IND:
        case IND_PERM:
        case IND_STATIC:
            /* do not print the '_' for indirections */
            fp[len] = '\0';
            /* could also be different type StgIndStatic */
            graphFingerPrint_(fp, visited, ((StgInd*)p)->indirectee);
            break;

        case RET_BCO:
        case RET_SMALL:
        case RET_BIG:
        case RET_FUN:
        case UPDATE_FRAME:
        case CATCH_FRAME:
        case UNDERFLOW_FRAME:
        case STOP_FRAME:
        case BLOCKING_QUEUE:
        case BLACKHOLE:
            // check if this is actually an indirection. See above in
            // packing code, some Blackholes are actually indirections
            // since ghc-7.0
            switch (((StgInfoTable*)
                     get_itbl(UNTAG_CLOSURE(((StgInd*)p)->indirectee)))->type) {
                case TSO:
                case BLOCKING_QUEUE:
                    debugBelch("Woops! Found blackhole during fingerprint!\n");
                    break;
                default:
                    /* do not print the '_' for indirections */
                    fp[len] = '\0';
                    graphFingerPrint_(fp, visited, ((StgInd*)p)->indirectee);
                    break;
            }
            break;

        case MVAR_CLEAN:
        case MVAR_DIRTY:
            if (((StgMVar *)p)->value != &stg_END_TSO_QUEUE_closure)
                graphFingerPrint_(fp, visited, ((StgMVar *)p)->value);
            break;

        case TVAR:
            // The TVAR type subsumes both the var itself and a watch
            // queue; the latter holds a TSO or an "Atomic Invariant"
            // where the former (clean/dirty) holds the current value
            // as its first payload. Anyways, while useful for GC, the
            // double meaning of the first payload is not useful for
            // fingerprinting. We do not descend into TVars.
            break;

        case ARR_WORDS:
            { // record size only (contains StgWords, not pointers)
                char str[6];
                sprintf(str, "%ld", (long) arr_words_words((StgArrWords*)p));
                strcat(fp, str);
            }
            break;

        case MUT_ARR_PTRS_CLEAN:
        case MUT_ARR_PTRS_DIRTY:
        case MUT_ARR_PTRS_FROZEN0:
        case MUT_ARR_PTRS_FROZEN:
            {
                char str[6];
                sprintf(str, "%ld", (long)((StgMutArrPtrs*)p)->ptrs);
                strcat(fp, str);
                nat i;
                for (i = 0; i < ((StgMutArrPtrs*)p)->ptrs; i++) {
                    //contains closures... follow
                    graphFingerPrint_(fp, visited,
                                      ((StgMutArrPtrs*)p)->payload[i]);
                }
                break;
            }
        case MUT_VAR_CLEAN:
        case MUT_VAR_DIRTY:
            graphFingerPrint_(fp, visited, ((StgMutVar *)p)->var);
            break;

        case WEAK:
        case PRIM:
            break;
        case MUT_PRIM:
            break;
        case TSO:
            break;
        case STACK:
            break;

        case TREC_CHUNK:
        case ATOMICALLY_FRAME:
        case CATCH_RETRY_FRAME:
        case CATCH_STM_FRAME:
        case WHITEHOLE:
            break;

#if __GLASGOW_HASKELL__ > 708
        case SMALL_MUT_ARR_PTRS_CLEAN:
        case SMALL_MUT_ARR_PTRS_DIRTY:
        case SMALL_MUT_ARR_PTRS_FROZEN0:
        case SMALL_MUT_ARR_PTRS_FROZEN:
            {
                char str[6];
                sprintf(str,"%ld",(long)((StgSmallMutArrPtrs*)p)->ptrs);
                strcat(fp,str);
                nat i;
                for (i = 0; i < ((StgSmallMutArrPtrs*)p)->ptrs; i++) {
                    //contains closures... follow
                    graphFingerPrint_(fp, visited,
                                      ((StgSmallMutArrPtrs*)p)->payload[i]);
                }
                break;
            }
#endif

        default:
            barf("graphFingerPrint_: unknown closure %d",
                 info -> type);
    }

}

//  Sanity check on a packet.
//    This does a full iteration over the packet, as in UnpackGraph.
//  Arguments: buffer data ptr, buffer size in words 
void pmcheckPacket(StgWord* buffer, nat size) {
    StgInt packsize, openptrs;
    nat clsize, ptrs, nonptrs, vhs;
    StgWord *bufptr;
    HashTable *offsets;

    IF_DEBUG(prof, debugBelch("checking packet (@ %p), size %ld words ...",
                              buffer, (long) size));

    offsets = allocHashTable(); // used to identify valid offsets
    packsize = 0; // compared against argument
    openptrs = 1; // counting pointers (but no need for a queue to fill them in)
    // initially, one pointer is open (graphroot)
    bufptr = buffer;

    do {
        StgWord tag;
        StgInfoTable *ip;

        ASSERT(*bufptr != END_OF_BUFFER_MARKER);

        // unpackclosure essentials are mimicked here
        tag = *bufptr; // marker in buffer (PLC | OFFSET | CLOSURE)

        if (tag == PLC) {
            bufptr++; // skip marker
            // check that this looks like a PLC (static data)
            // which is however complicated when code and data mix... TODO

            bufptr++; // move forward
            packsize += 2;
        } else if (tag == OFFSET) {
            bufptr++; // skip marker
            if (!lookupHashTable(offsets, *bufptr)) {
                barf("invalid offset %" FMT_Word " in packet "
                        " at position %p", *bufptr,  bufptr);
            }
            bufptr++; // move forward
            packsize += 2;
        } else if (tag == CLOSURE) {
            bufptr++; // skip marker

            // untag info offset and compute info pointer (first word of the
            // closure), then compute a proper info table
            ip = UNTAG_CAST(StgInfoTable*,P_POINTER(*bufptr));

            // check info ptr
            if (!LOOKS_LIKE_INFO_PTR((StgWord) ip)) {
                barf("Non-closure found in packet"
                        " at position %p (value %p)\n",
                         bufptr, ip);
            }

            // analogous to unpacking, we pretend the buffer is a heap closure
            ip = getClosureInfo((StgClosure*) bufptr, INFO_PTR_TO_STRUCT(ip),
                                &clsize, &ptrs, &nonptrs, &vhs);

            // IF_DEBUG(sparks,debugBelch("size (%ld + %d + %d +%d, = %d)",
            //              HEADERSIZE, vhs, ptrs, nonptrs, clsize));

            // This is rather a test for getClosureInfo...but used here
            if (clsize != (nat) HEADERSIZE + vhs + ptrs + nonptrs) {
                barf("size mismatch in packed closure at %p :"
                     "(%d + %d + %d +%d != %d)", bufptr,
                     HEADERSIZE, vhs, ptrs, nonptrs, clsize);
            }

            // do a plausibility check on the values. Assume we never see
            // large numbers of ptrs and non-ptrs simultaneously
            if (ptrs > 99 && nonptrs > 99) {
                barf("Found weird infoptr %p in packet "
                        " (position %p): vhs %d, %d ptrs, %d non-ptrs, size %d",
                        ip, bufptr, vhs, ptrs, nonptrs, clsize);
            }

            // Register the pack location as a valid offset. Offsets are
            // one-based (we incremented bufptr before) in units of StgWord
            // (which is magically adjusted by the C compiler here!).
            insertHashTable(offsets,
                    (StgWord) ( bufptr - buffer ),
                    bufptr);  // No need to store any value

            switch (ip->type) {
                // some closures need special treatment, as their size in the
                // packet is unobvious

                case PAP:
                    // all arg.s packed as tag + value, 2 words per arg.
                    bufptr += sizeofW(StgHeader) + 1 + 2*((StgPAP*) bufptr)->n_args;
                    packsize += 1 + sizeofW(StgHeader) + 1 + 2*((StgPAP*) bufptr)->n_args;
                    break;
                case AP: // same, but thunk header
                    bufptr += sizeofW(StgThunkHeader) + 1 + 2*((StgAP*) bufptr)->n_args;
                    packsize += 1 + sizeofW(StgThunkHeader) + 1 + 2*((StgAP*) bufptr)->n_args;
                    break;
                case MUT_ARR_PTRS_CLEAN:
                case MUT_ARR_PTRS_DIRTY:
                case MUT_ARR_PTRS_FROZEN0:
                case MUT_ARR_PTRS_FROZEN:
                    // card table is counted as non-pointer, but not in packet
                    bufptr += sizeofW(StgHeader) + vhs;
                    packsize += 1 + sizeofW(StgHeader) + vhs;
                    break;
                default: // standard (ptrs. first) layout
                    bufptr += HEADERSIZE + vhs + nonptrs;
                    packsize += (StgInt) 1 + HEADERSIZE + vhs + nonptrs;
            }

            openptrs += (StgInt) ptrs; // closure needs some pointers to be filled in
        } else {
            barf("found invalid tag %x in packet", *bufptr);
        }

        openptrs--; // one thing was unpacked

    } while (openptrs != 0 && packsize < size);

    IF_DEBUG(prof,
            debugBelch(" traversed %" FMT_Word " words.", packsize));

    if (openptrs != 0) {
        barf("%d open pointers at end of packet ",
                openptrs);
    }

    IF_DEBUG(sanity, ASSERT(*(bufptr++) == END_OF_BUFFER_MARKER && packsize++));

    if (packsize != size) {
        barf("surplus data (%" FMT_Word " words) at end of packet ",
             size - packsize);
    }

    freeHashTable(offsets, NULL);
    IF_DEBUG(prof, debugBelch("packet OK\n"));

}

/* END OF DEBUG */
#endif

// Local Variables:
// mode: C++
// fill-column: 80
// indent-tabs-mode: nil
// c-basic-offset: 4
// buffer-file-coding-system: utf-8-unix
// End:
