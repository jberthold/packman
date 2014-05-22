/*
   Packing for the Generic RTE:
--------

   Graph packing and unpacking code for sending it to another processor
   and retrieving the original graph structure from the packet.
   Used in GUM and Eden.

   (Outdated) Documentation for heap closures can be found at
   http://hackage.haskell.org/trac/ghc/wiki/Commentary/Rts/Storage/HeapObjects
   However, the best documentation is includes/Closure*h and rts/sm/Scav.c
*/

#include <Rts.h>
#include <string.h>

#include "Types.h"
#include "Hash.h"
#include "Errors.h"
#include "Utils.h"



#define messageBlackHole(c,s)  0
#define DEBUG_HEADROOM  2
#define IF_PAR_DEBUG(c,s)


/* -----------------------------------------------------------------------------
   Closure types

   NOTE: must be kept in sync with the closure types in includes/ClosureTypes.h
   -------------------------------------------------------------------------- */

char *closure_type_names2[] = {
    [INVALID_OBJECT]        = "INVALID_OBJECT",
    [CONSTR]                = "CONSTR",
    [CONSTR_1_0]            = "CONSTR_1_0",
    [CONSTR_0_1]            = "CONSTR_0_1",
    [CONSTR_2_0]            = "CONSTR_2_0",
    [CONSTR_1_1]            = "CONSTR_1_1",
    [CONSTR_0_2]            = "CONSTR_0_2",
    [CONSTR_STATIC]         = "CONSTR_STATIC",
    [CONSTR_NOCAF_STATIC]   = "CONSTR_NOCAF_STATIC",
    [FUN]                   = "FUN",
    [FUN_1_0]               = "FUN_1_0",
    [FUN_0_1]               = "FUN_0_1",
    [FUN_2_0]               = "FUN_2_0",
    [FUN_1_1]               = "FUN_1_1",
    [FUN_0_2]               = "FUN_0_2",
    [FUN_STATIC]            = "FUN_STATIC",
    [THUNK]                 = "THUNK",
    [THUNK_1_0]             = "THUNK_1_0",
    [THUNK_0_1]             = "THUNK_0_1",
    [THUNK_2_0]             = "THUNK_2_0",
    [THUNK_1_1]             = "THUNK_1_1",
    [THUNK_0_2]             = "THUNK_0_2",
    [THUNK_STATIC]          = "THUNK_STATIC",
    [THUNK_SELECTOR]        = "THUNK_SELECTOR",
    [BCO]                   = "BCO",
    [AP]                    = "AP",
    [PAP]                   = "PAP",
    [AP_STACK]              = "AP_STACK",
    [IND]                   = "IND",
    [IND_PERM]              = "IND_PERM",
    [IND_STATIC]            = "IND_STATIC",
    [RET_BCO]               = "RET_BCO",
    [RET_SMALL]             = "RET_SMALL",
    [RET_BIG]               = "RET_BIG",
    [RET_FUN]               = "RET_FUN",
    [UPDATE_FRAME]          = "UPDATE_FRAME",
    [CATCH_FRAME]           = "CATCH_FRAME",
    [UNDERFLOW_FRAME]       = "UNDERFLOW_FRAME",
    [STOP_FRAME]            = "STOP_FRAME",
    [BLOCKING_QUEUE]        = "BLOCKING_QUEUE",
    [BLACKHOLE]             = "BLACKHOLE",
    [MVAR_CLEAN]            = "MVAR_CLEAN",
    [MVAR_DIRTY]            = "MVAR_DIRTY",
    [TVAR]                  = "TVAR",
    [ARR_WORDS]             = "ARR_WORDS",
    [MUT_ARR_PTRS_CLEAN]    = "MUT_ARR_PTRS_CLEAN",
    [MUT_ARR_PTRS_DIRTY]    = "MUT_ARR_PTRS_DIRTY",
    [MUT_ARR_PTRS_FROZEN0]  = "MUT_ARR_PTRS_FROZEN0",
    [MUT_ARR_PTRS_FROZEN]   = "MUT_ARR_PTRS_FROZEN",
    [MUT_VAR_CLEAN]         = "MUT_VAR_CLEAN",
    [MUT_VAR_DIRTY]         = "MUT_VAR_DIRTY",
    [WEAK]                  = "WEAK",
    [PRIM]                  = "PRIM",
    [MUT_PRIM]              = "MUT_PRIM",
    [TSO]                   = "TSO",
    [STACK]                 = "STACK",
    [TREC_CHUNK]            = "TREC_CHUNK",
    [ATOMICALLY_FRAME]      = "ATOMICALLY_FRAME",
    [CATCH_RETRY_FRAME]     = "CATCH_RETRY_FRAME",
    [CATCH_STM_FRAME]       = "CATCH_STM_FRAME",
    [WHITEHOLE]             = "WHITEHOLE"
};

char *
info_type2(StgClosure *closure) {
    return closure_type_names2[get_itbl(closure)->type];
}

char *
info_type_by_ip2(StgInfoTable *ip) {
    return closure_type_names2[ip->type];
}

#define info_type  info_type2
#define info_type_by_ip  info_type_by_ip2

/* later:
#include "RTTables.h" // packet split operates on inports,
                      // needs types and methods
*/


/* nat messageBlackHole(Capability *cap, MessageBlackHole *msg) { */
    /* return 0; */
/* } */


// for better reading only... ATTENTION: given in bytes!
/* #define RTS_PACK_BUFFER_SIZE   RtsFlags.ParFlags.packBufferSize */
#define RTS_PACK_BUFFER_SIZE   10485760   // 10 MiB


// size of the (fixed) Closure header in words
#define HEADERSIZE sizeof(StgHeader)/sizeof(StgWord)

// some sizes for packed parts of closures
#define PACK_PLC_SIZE	2	/* Size of a packed PLC in words */
#define PACK_GA_SIZE	3	/* Size of a packed GA in words */
#define PACK_FETCHME_SIZE (PACK_GA_SIZE + HEADERSIZE)
			        /* Size of a packed fetch-me in words */

// markers for packed/unpacked type
#define PLC     0L
#define OFFSET  1L
#define CLOSURE 2L

#define END_OF_BUFFER_MARKER 0xedededed

// forward declarations:

/* Tagging macros will work for any word-sized type, not only
  closures. In the packet, we tag info pointers instead of
  closure pointers.
  See "pointer tagging" before "PackNearbyGraph" routine for use.
*/
#define UNTAG_CAST(type,p) ((type) UNTAG_CLOSURE((StgClosure*) (p)))

/* Info pointer <--> Info offset (also for PLC pointers)
   See "relocatable binaries" before "PackNearbyGraph" routine for use.
*/
#define BASE_SYM ZCMain_main_info // base symbol for offset
extern const StgInfoTable BASE_SYM[];

// use this one on info pointers before they go into a packet
#define P_OFFSET(ip) ((StgWord) ((StgWord) (ip)) - (StgWord) BASE_SYM)
// use this one on info offsets taken from packets
#define P_POINTER(val) ((StgWord)(val) + (StgWord) BASE_SYM)

//   ADT of closure queues
STATIC_INLINE void InitClosureQueue(void);
STATIC_INLINE void StuffClosureQueue(void);
STATIC_INLINE rtsBool QueueEmpty(void);
STATIC_INLINE nat QueueSize(void);
STATIC_INLINE void QueueClosure(StgClosure *closure);
STATIC_INLINE StgClosure *DeQueueClosure(void);

//   Init for packing
static void InitPacking(rtsBool unpack);
static void ClearPackBuffer(void);

// de-init:
static void DonePacking(void);

// little helpers:
STATIC_INLINE void RegisterOffset(StgClosure *closure);
STATIC_INLINE StgWord OffsetFor(StgClosure *closure);
STATIC_INLINE rtsBool AlreadyPacked(int offset);
STATIC_INLINE StgInfoTable* get_closure_info(StgClosure* node, StgInfoTable* info,
                                             nat *size, nat *ptrs,
                                             nat *nonptrs, nat *vhs);

// declared in Parallel.h
// rtsBool IsBlackhole(StgClosure* closure);


/* used here and by the primitive which creates new channels:
   creating a blackhole closure from scratch.
   Declared in Parallel.h
StgClosure* createBH(Capability *cap);

   used in HLComms: creating a list node
   Declared in Parallel.h
StgClosure* createListNode(Capability *cap,
                           StgClosure *head, StgClosure *tail);
*/

/* TODO: for packet splitting, make RoomToPack a procedure, which
   always succeeds, by sending partial data away when no room is
   left. => Omit ptrs argument.
*/
STATIC_INLINE rtsBool RoomToPack(nat size);

// Packing and helpers

// external interface, declared in Parallel.h:
// pmPackBuffer* PackNearbyGraph(StgClosure* closure, StgTSO* tso);

// packing routine, branches into special cases
static StgWord PackClosure(StgClosure *closure);
// packing static addresses and offsets
STATIC_INLINE void PackPLC(StgPtr addr);
STATIC_INLINE void PackOffset(StgWord offset);
// the standard case: a heap-alloc'ed closure
static StgWord PackGeneric(StgClosure *closure);

// special cases:
static StgWord PackPAP(StgPAP *pap);
static StgWord PackArray(StgClosure* array);

// low-level packing: fill one StgWord of data into the globalPackBuffer
STATIC_INLINE void Pack(StgWord data);


// Unpacking routines:

// unpacking state (saved & restored)
/* this structure saves internal data from Pack.c.
 * Not used outside, only saved as an StgPtr in the inport structure
 */
typedef struct UnpackInfo_ {
    StgClosure *parent;         // current parent
    nat pptr;                   // current child pointer
    nat pptrs;                  // no. of pointers
    nat  pvhs;                  // var. hdr. size (offset for filling in ptrs.)
    StgClosure* graphroot;      // for GC (hard but true: always evacuate the whole
                                // graph, since following message can contain
                                // offset references
    nat queue_length;
    StgClosure** queue;         // closure queue, variable size
    nat offsetpadding;          // padding to adjust offset between several packets
    HashTable* offsets;         // set of offsets, stored in a Hashtable
} UnpackInfo;

/* Future use: global unpack state to support fragmented subgraphs
static StgClosure* restoreUnpackState(UnpackInfo* unpack,StgClosure** graphroot,
				      nat* pptr, nat* pptrs, nat* pvhs);
static UnpackInfo* saveUnpackState(StgClosure* graphroot, StgClosure* parent,
				   nat pptr, nat pptrs, nat pvhs);
*/

// external interface, declared in Parallel.h:
/*
StgClosure        *UnpackGraph(pmPackBuffer *packBuffer,
			       Port inPort,
			       Capability* cap);
*/
// internal function working on the raw data (instead of pmPackBuffer)
StgClosure* UnpackGraph_(StgWord *buffer, StgInt size, Capability* cap);

// unpacks one closure (common prelude + switches to special cases)
static  StgClosure *UnpackClosure(StgWord **bufptrP, Capability* cap);

// normal case:
STATIC_INLINE void LocateNextParent(StgClosure **parentP, nat *pptrP,
                                    nat *pptrsP, nat *pvhsP);
// special cases:
STATIC_INLINE  StgClosure *UnpackOffset(StgWord **bufptrP);
STATIC_INLINE  StgClosure *UnpackPLC(StgWord **bufptrP);
static StgClosure *UnpackPAP(StgInfoTable *ip,StgWord **bufptrP,
                             Capability* cap);
static StgClosure *UnpackArray(StgInfoTable *info, StgWord **bufptrP,
                               Capability* cap);

/* A special structure used as the "owning thread" of system-generated
 * blackholes.  Layout [ hdr | payload ], holds a TSO header.info and blocking
 * queues in the payload field.
 *
 * Used in: createBH (here),
 * Threads::updateThunk + Messages::messageBlackHole (special treatment)
 * ParInit::synchroniseSystem(init),
 * Evac::evacuate (do not evacuate) and GC::garbageCollect (evac. BQueue)
 */
StgInd stg_system_tso;


/* Global (static) variables and declarations: As soon as we allow
   threaded+parallel, we need a lock, or all these will become fields
   in a thread-local buffer structure.

   Given the amount of static variables in this code, we go with the lock
   solution as a first version.
*/
#if defined(THREADED_RTS)
Mutex pack_mutex;
#endif

/* The pack buffer, space for packing a graph, protected by pack_mutex */
static pmPackBuffer *globalPackBuffer = NULL;

/* packing and unpacking misc: */
static nat     pack_locn,           /* ptr to first free loc in pack buffer */
               buf_id = 1;          /* identifier for buffer */
static nat     unpacked_size;

/* The offset hash table is used during packing to record the location
   in the pack buffer of each closure which is packed */
static HashTable *offsetTable;

static nat offsetpadding = 0; // padding for offsets in subsequent packets
/* the closure queue */
static StgClosure **ClosureQueue = NULL;
static nat        clq_size, clq_pos;

#if defined(DEBUG)
// finger print: "type hash" of packed graph, for quick debugging
// checks
#define MAX_FINGER_PRINT_LEN  1023
static char fingerPrintStr[MAX_FINGER_PRINT_LEN];
static void GraphFingerPrint(StgClosure *graphroot);
static HashTable *tmpClosureTable;  // used in GraphFingerPrint and PrintGraph

void checkPacket(pmPackBuffer *packBuffer);
#endif

// functionality:

//   utilities and helpers

/* @initPackBuffer@ initialises the packing buffer etc. called at startup */
void InitPackBuffer(void)
{
    ASSERT(RTS_PACK_BUFFER_SIZE > 0);

    if (globalPackBuffer==(pmPackBuffer*)NULL) {
        if ((globalPackBuffer = (pmPackBuffer *)
                    pmMallocBytes(sizeof(pmPackBuffer)
                        + RTS_PACK_BUFFER_SIZE
                        + sizeof(StgWord)*DEBUG_HEADROOM,
                        "InitPackBuffer")) == NULL) {
            barf("InitPackBuffer: could not allocate.");
        }
    }

#if defined(THREADED_RTS)
    initMutex(&pack_mutex);
#endif

    // we must retain all CAFs, as packet data might refer to it.
    // This variable lives in Storage.c, inhibits GC for CAFs.
    keepCAFs = rtsTrue;
}

void freePackBuffer(void)
{
    if (globalPackBuffer) // has been allocated (called from ParInit, so always)
        pmFree(globalPackBuffer);
    if (ClosureQueue) // has been allocated
        pmFree(ClosureQueue);
}

//@cindex InitPacking
static void InitPacking(rtsBool unpack)
{
    if (globalPackBuffer == NULL) {
        InitPackBuffer();
    }
    if (unpack) {
        /* allocate a GA-to-GA map (needed for ACK message) */
        // InitPendingGABuffer(RtsFlags.ParFlags.packBufferSize);
    }

    /* init queue of closures seen during packing */
    InitClosureQueue();

    offsetpadding = 1; // will be modified after sending partial message
                       // We start at 1 for the case that the graph root
                       // (with pack_locn=0) is found again.

    // we need to store and recall offsets quickly also when unpacking
    // partially filled packets.
    offsetTable = allocHashTable();
    if (unpack)
        return;

    pack_locn = 0;         /* the index into the actual pack buffer */
    unpacked_size = 0;     /* the size of the whole graph when unpacked */
}

/* clear buffer, but use the old queue (stuffed) and the old offset table
   essentially a copy of InitPacking without offsetTable
   important: recall old pack_location as "offsetpadding" to allow
   cross-packet offsets. */
static void ClearPackBuffer(void)
{
    // no need to allocate memory again
    ASSERT(globalPackBuffer != NULL);
    ASSERT(ClosureQueue != NULL);

    // stuff the closure queue (would soon be full if we just continue)
    StuffClosureQueue();

    offsetpadding += pack_locn; // set to 1 when started (un)packing...

    // Buffer remains the same, admin. fields invalidated
    pack_locn = 0;         // the index into the actual pack buffer
    unpacked_size = 0;     // the size of the whole graph when unpacked
}

/* DonePacking is called when we've finished packing.  It releases
   memory etc.  */

static void DonePacking(void)
{
    freeHashTable(offsetTable, NULL);
    offsetTable = NULL;
    offsetpadding = 0; // which is invalid.
}

/* RegisterOffset records that/where the closure is packed. */
STATIC_INLINE void
RegisterOffset(StgClosure *closure)
{
    insertHashTable(offsetTable,
            // remove tag for offset
            UNTAG_CAST(StgWord, closure),
            (void *) (StgWord) (pack_locn + offsetpadding));
    // note: offset is never 0, padding starts at 1
}

/* OffsetFor returns an offset for a closure which is already being packed. */
STATIC_INLINE StgWord
OffsetFor(StgClosure *closure)
{
    // avoid typecast warnings...
    void* offset;
    offset = (lookupHashTable(offsetTable,
                // remove tag for offset
                UNTAG_CAST(StgWord, closure)));
    return (StgWord) offset;
}

/* AlreadyPacked determines whether the closure's already being packed.
   Offset == 0 means no.  */
STATIC_INLINE rtsBool
AlreadyPacked(int offset)
{
    // When root is found again, it will have offset 1 (offsetpadding).
    return(offset != 0);
}

/***************************************************************
 * general helper functions used here:
 * Perhaps find a different home for some of them?
 * ----------------------------------------------------------- */

/*  get_closure_info: returns payload structure for closures.
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
get_closure_info(StgClosure* node, StgInfoTable* info,
                 nat *size, nat *ptrs, nat *nonptrs, nat *vhs)
{
    /* We remove the potential tag before doing anything. */
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

        /* PAP/AP/AP_STACK contain a function field,
           treat this field as a (= the one single) pointer */
        case PAP:
            *vhs = 1; /* arity/args */
            *ptrs = 1;
            /* wrong (some are ptrs), but not used in the unpacking code! */
            *nonptrs = *size - 2 - sizeofW(StgHeader);
            break;

        case AP_STACK:
        case AP:
            *vhs = sizeofW(StgThunkHeader) - sizeofW(StgHeader) + 1;
            *ptrs = 1;
            /* wrong (some are ptrs), but not used in the unpacking code! */
            *nonptrs = *size - *vhs - 1;
            break;

        /* For Word arrays, no pointers need to be filled in.
         * (the default case would work for them as well)
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

        /* Small arrays do not have card tables, straightforward. */
        /* case SMALL_MUT_ARR_PTRS_CLEAN: */
        /* case SMALL_MUT_ARR_PTRS_DIRTY: */
        /* case SMALL_MUT_ARR_PTRS_FROZEN0: */
        /* case SMALL_MUT_ARR_PTRS_FROZEN: */
        /*   *vhs = 1; // ptrs field */
        /*   *ptrs = ((StgSmallMutArrPtrs*) node)->ptrs; */
        /*   *nonptrs = 0; */
        /*   break; */

        /* we do not want to see these here (until thread migration) */
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
            barf("get_closure_info: stack frame!");
            break;

        default:
            /* this works for all pointers-first layouts */
            *ptrs = (nat) (info->layout.payload.ptrs);
            *nonptrs = (nat) (info->layout.payload.nptrs);
            *vhs = *size - *ptrs - *nonptrs - sizeofW(StgHeader);
    }

    ASSERT(*size == sizeofW(StgHeader) + *vhs + *ptrs + *nonptrs);

    return info;
}

/* quick test for blackholes. Available somewhere else? */
rtsBool IsBlackhole(StgClosure* node)
{
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

StgClosure* createBH(Capability *cap)
{
    StgClosure *new;

    // a blackhole carries one pointer of payload, see StgMiscClosures.cmm, so
    // we allocate 2 words. The payload indicates the blackhole owner, in our
    // case it is the "system" (or later, the cap, for -threaded rts).
    new = (StgClosure*) allocate(cap, 2);

    SET_HDR(new, &stg_BLACKHOLE_info, CCS_SYSTEM); // ccs to be checked!

    new->payload[0] = (StgClosure*) &stg_system_tso;
    // see above. Pseudo-TSO (has TSO info pointer) owning all
    // system-created black holes, and storing BQs.

    return new;
}

// cons node info pointer, from GHC.Base
#define CONS_INFO ghczmprim_GHCziTypes_ZC_con_info
// constructor tag for pointer tagging. We return a tagged pointer here!
#define CONS_TAG  2
extern const StgInfoTable CONS_INFO[];

// creating a list node. returns a tagged pointer.
StgClosure* createListNode(Capability *cap, StgClosure *head, StgClosure *tail)
{
    StgClosure *new;

    // a list node (CONS) carries two pointers => 3 words to allocate
    // if we have given a capability, we can allocateLocal (cheaper, no lock)
    new = (StgClosure*) allocate(cap, 3);

    SET_HDR(new, CONS_INFO, CCS_SYSTEM); // to be checked!!!
    new->payload[0] = head;
    new->payload[1] = tail;

    return TAG_CLOSURE(CONS_TAG,new);
}


/*
  RoomToPack determines whether there's room to pack the closure into
  the pack buffer. The buffer must be able to hold at least <size>
  more StgWords, plus one StgWord - the type tag (PLC,OFFSET, CLOSURE).

  Otherwise, it sends partial data away when no room is left.
  For GUM, we will have to include the queue size (as FETCH_MEs) as well.
*/
STATIC_INLINE rtsBool RoomToPack(nat size)
{
    if (((pack_locn +                 // where we are in the buffer right now
                    size +            // space needed for the current closure
                    // for GUM:
                    // QueueSize * sizeof(FETCH_ME-in-buffer)
                    1) * sizeof(StgWord)          // tag
                >=
                RTS_PACK_BUFFER_SIZE))
    {
        IF_PAR_DEBUG(pack,
                debugBelch("Pack buffer full (size %d). "
                    "Sending partially to receiver.",
                    pack_locn));
        errorBelch("Runtime system pack buffer full.\n"
                "Current buffer size is %lu bytes, "
                "try bigger pack buffer with +RTS -qQ<size>",
                (long unsigned int) RTS_PACK_BUFFER_SIZE);
        return rtsFalse;
    }
    return rtsTrue;
}


// Closure Queue:
/* InitClosureQueue allocates and initialises the closure queue. */
STATIC_INLINE void InitClosureQueue(void)
{
    clq_pos = clq_size = 0;

    if (ClosureQueue==NULL) {
        ClosureQueue = (StgClosure**)
            pmMallocBytes(RTS_PACK_BUFFER_SIZE,
                          "InitClosureQueue");
    }
}

/* PrintClosureQueue prints the whole closure queue. */
#if defined(DEBUG)
static void PrintClosureQueue(void)
{
    nat i;

    debugBelch("Closure queue:\n");
    for (i=clq_pos; i < clq_size; i++) {
        debugBelch("at %d: %p (%s), \n",
                clq_size-i,
                (StgClosure *)ClosureQueue[i],
                info_type(UNTAG_CLOSURE(ClosureQueue[i])));
    }
}
#endif

/* StuffClosureQueue moves the enqueued closures to the beginning of
   the allocated area (could use a modulo instead, easier like
   this, since not common case) */
STATIC_INLINE void StuffClosureQueue(void)
{

    ASSERT(ClosureQueue != NULL);
    ASSERT(clq_pos<=clq_size);
    IF_PAR_DEBUG(packet,
            debugBelch("Stuffing closure queue (length %d).", QueueSize());
            PrintClosureQueue());
    if (clq_pos < clq_size) {
        // move content of queue to start of allocated memory (if any content)
        memmove(ClosureQueue, ClosureQueue + clq_pos,
                (clq_size - clq_pos) * sizeof(StgClosure*));
    }
    // adjust position and size
    clq_size = clq_size - clq_pos;
    clq_pos = 0;
    IF_PAR_DEBUG(packet,
            debugBelch("Closure queue now:");
            PrintClosureQueue());
    return;
}

/* QueueEmpty is rtsTrue if closure queue empty; rtsFalse otherwise */
STATIC_INLINE rtsBool QueueEmpty(void)
{
    ASSERT(clq_pos <= clq_size);
    return(clq_pos == clq_size);
}

/* QueueSize is the queue size */
STATIC_INLINE nat QueueSize(void)
{
    ASSERT(clq_pos <= clq_size);
    return(clq_size - clq_pos);
}

/* QueueClosure adds its argument to the closure queue. */
STATIC_INLINE void QueueClosure(StgClosure* closure)
{
    ASSERT(clq_pos <= clq_size);
    if (clq_size < RTS_PACK_BUFFER_SIZE/sizeof(StgClosure*)) {
        IF_PAR_DEBUG(packet,
                debugBelch(">__> Q: %p (%s); %ld elems in q\n",
                    closure,
                    info_type(UNTAG_CLOSURE(closure)), (long)clq_size-clq_pos+1));
        ClosureQueue[clq_size++] = closure;
    } else {
        if (clq_pos != 0) {
            StuffClosureQueue();
            QueueClosure(closure);
        } else {
            errorBelch("Pack.c: Closure Queue Overflow (enqueueing %p (%s)"
                    ", %ld elem.s in queue).\nCurrent size %lu bytes,"
                    " try increasing with RTS flag -qQ<size>",
                    closure,
                    info_type(UNTAG_CLOSURE(closure)), (long)clq_size-clq_pos+1,
                    (long unsigned int) RTS_PACK_BUFFER_SIZE);
            stg_exit(EXIT_FAILURE);
        }
    }
}

/* DeQueueClosure returns the head of the closure queue. */
STATIC_INLINE StgClosure* DeQueueClosure(void)
{
    if (!QueueEmpty()) {
        IF_PAR_DEBUG(packet,
                debugBelch(">__> DeQ: %p (%s); %ld elems in q\n",
                    ClosureQueue[clq_pos],
                    info_type(UNTAG_CLOSURE(ClosureQueue[clq_pos])),
                    (long)clq_size-clq_pos-1));
        return (ClosureQueue[clq_pos++]);
    } else {
        IF_PAR_DEBUG(packet, debugBelch("Q empty\n "));
        return ((StgClosure*)NULL);
    }
}

/*******************************************************************
 * packing a graph structure:
 *
 * The graph is packed breadth-first into a static buffer.
 *
 * Interface: PackNearbyGraph(IN graph_root, IN packing_tso)
 *
 * main functionality: PackClosure (switches over type)
 *                     PackGeneric (copies, follows generic layout)
 *
 * Packing uses a global pack buffer and a number of global variables, and
 * the global buffer is returned. Therefore pack_mutex lock must be held
 * when calling packNearbyGraph, and as long as its return values is alive.
 *
 * mid-level packing: a closure is preceded by a marker for its type
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

// helper accessing the pack buffer
STATIC_INLINE void Pack(StgWord data)
{
    ASSERT(pack_locn*sizeof(StgWord) < RTS_PACK_BUFFER_SIZE);
    globalPackBuffer->buffer[pack_locn++] = data;
}

// packing a static value
STATIC_INLINE void PackPLC(StgPtr addr)
{
    Pack(PLC);                     /* weight */
    // pointer tag of addr still present, packed as-is (with offset)
    Pack((StgWord) P_OFFSET(addr)); /* address */
}

// packing an offset (repeatedly packed same closure)
STATIC_INLINE void PackOffset(StgWord offset)
{
    Pack(OFFSET);       /* weight */
    //  Pack(0L);       /* pe */
    Pack(offset);       /* slot/offset */
}

/*
  Packing Sections of Nearby Graph

  PackNearbyGraph packs a closure and associated graph into a static
  buffer (PackBuffer).  It returns the address of this buffer (which
  includes the size of the data packed into it, buffer->size !). The
  associated graph is packed in a breadth-first manner, hence it uses
  an explicit queue of closures to be packed rather than simply using
  a recursive algorithm.

  A request for packing may be accompanied by the actual TSO. The TSO
  can then be enqueued, if packing hits a black hole (meaning that we
  retry packing when these data have been evaluated.

  */

pmPackBuffer* PackNearbyGraph(StgClosure* closure, StgTSO* tso)
{
    StgWord errcode = P_SUCCESS; // error code returned by PackClosure

    InitPacking(rtsFalse);

    IF_PAR_DEBUG(verbose,
            debugBelch("Packing subgraph @ %p\n", closure));

    IF_PAR_DEBUG(pack,
            debugBelch("packing:");
            debugBelch("id <%ld> (buffer @ %p); graph root @ %p [PE %d]\n",
                (long)globalPackBuffer->id, globalPackBuffer,
                closure, thisPE);
            GraphFingerPrint(closure);
            debugBelch("    demanded by TSO %d (%p); Fingerprint is\n"
                "\t{%s}\n",
                (int)(tso?tso->id:0), tso, fingerPrintStr));
#if !defined(PARALLEL_RTS)
    IF_DEBUG(scheduler,
            debugBelch("packing:");
            debugBelch("id <%ld> (buffer @ %p); graph root @ %p\n",
                (long)globalPackBuffer->id, globalPackBuffer,
                closure);
            GraphFingerPrint(closure);
            debugBelch("    demanded by TSO %d (%p); Fingerprint is\n"
                "\t{%s}\n",
                (int)(tso?tso->id:0), tso, fingerPrintStr));
#endif

    QueueClosure(closure);
    do {
        errcode = PackClosure(DeQueueClosure());
        if (errcode != P_SUCCESS) {
            DonePacking();
            return ((pmPackBuffer *) errcode);
        }
    } while (!QueueEmpty());


    /* Check for buffer overflow (again) */
    ASSERT((pack_locn - DEBUG_HEADROOM) * sizeof(StgWord)
            <= RTS_PACK_BUFFER_SIZE);
    IF_DEBUG(sanity, // write magic end-of-buffer word
            globalPackBuffer->buffer[pack_locn++] = END_OF_BUFFER_MARKER);

    /* Record how much space the graph needs in packet and in heap */
    globalPackBuffer->unpacked_size = unpacked_size;
    globalPackBuffer->size = pack_locn;

    /* done packing */
    DonePacking();

    IF_PAR_DEBUG(pack,
            debugBelch("** Finished <<%ld>> packing graph %p (%s); packed size: %ld; size of graph: %ld\n",
                (long)globalPackBuffer->id, closure, info_type(UNTAG_CLOSURE(closure)),
                (long)globalPackBuffer->size,
                (long)globalPackBuffer->unpacked_size));;

    IF_DEBUG(sanity, checkPacket(globalPackBuffer));

    return (globalPackBuffer);
}

STATIC_INLINE StgClosure* UNWIND_IND(StgClosure *closure)
{
    StgClosure *start = closure;

    while (closure_IND(start))
        start = ((StgInd*) UNTAG_CLOSURE(start))->indirectee;

    return start;
}


/*
  @PackClosure@ is the heart of the normal packing code.  It packs a
  single closure into the pack buffer, skipping over any indirections,
  queues any child pointers for further packing.

  the routine returns error codes (see includes/rts/Parallel.h) indicating
  error status when packing a closure fails.
*/

static StgWord PackClosure(StgClosure* closure)
{

    StgInfoTable *info;
    StgWord offset;

    // Ensure we can always pack this closure as an offset/PLC.
    if (!RoomToPack(sizeofW(StgWord)))
        return P_NOBUFFER;

loop:
    closure = UNWIND_IND(closure);
    /* now closure is the thing we want to pack */
    // ... but might still be tagged.
    offset = OffsetFor(closure);

    /* If the closure has been packed already, just pack an indirection to it
       to guarantee that the graph doesn't become a tree when unpacked */
    if (AlreadyPacked(offset)) {
        PackOffset(offset);
        return P_SUCCESS;
    }

    // remove the tag (temporary, subroutines will handle tag as needed)
    info = get_itbl(UNTAG_CLOSURE(closure));

    // we rely on info-pointers being word-aligned!
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
            return PackGeneric(closure);

        case CONSTR_STATIC:        // We ship indirections to CAFs: They are
        case CONSTR_NOCAF_STATIC:  // evaluated on each PE if needed
        case FUN_STATIC:
        case THUNK_STATIC:
            // all these are packed with their tag (closure is still tagged here)
            IF_PAR_DEBUG(packet,
                    debugBelch("*>~~ Packing a %p (%s) as a PLC\n",
                        closure, info_type_by_ip(info)));

            PackPLC((StgPtr)closure);
            // NB: unpacked_size of a PLC is 0
            return P_SUCCESS;

        case FUN:
        case FUN_1_0:
        case FUN_0_1:
        case FUN_2_0:
        case FUN_1_1:
        case FUN_0_2:
            return PackGeneric(closure);

        case THUNK:
        case THUNK_1_0:
        case THUNK_0_1:
        case THUNK_2_0:
        case THUNK_1_1:
        case THUNK_0_2:
            // !different layout! (smp update field, see Closures.h)
            // the update field should better not be shipped...
            return PackGeneric(closure);

        case THUNK_SELECTOR:
            /* a thunk selector means we want to extract one of the
             * arguments of another closure. See GC.c::eval_thunk_selector:
             * selectee might be CONSTR*, or IND*, or unevaluated (THUNK*,
             * AP, AP_STACK, BLACKHOLE).
             *
             * GC tries to evaluate and eliminate THUNK_SELECTORS by
             * following them. For packing, we could include them in
             * UNWIND_IND, but this is fatal in case of a loop (UNWIND_IND
             * will loop). So we just pack the selectee as well
             * instead. get_closure_info treats the selectee in this closure
             * type as a pointer field.
             */

            IF_PAR_DEBUG(packet,
                    StgClosure *selectee
                    = ((StgSelector *) UNTAG_CLOSURE(closure))->selectee;
                    debugBelch("*>** Found THUNK_SELECTOR at %p (%s)"
                        "pointing to %p (%s)\n",
                        closure, info_type_by_ip(info),
                        selectee, info_type(UNTAG_CLOSURE(selectee))));
            return PackGeneric(closure);

        case BCO:
            goto unsupported;

        case AP:
        case PAP:
            return PackPAP((StgPAP *)closure); // also handles other stack-containing types

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
                    case TSO: // no blocking queue yet. msgBlackHole will create one.
                    case BLOCKING_QUEUE: // another thread already blocked here. Enqueue

                        // If a TSO called a primOp, it must be blocked on this BH
                        // until the BH gets updated/data arrives. On the awakening of
                        // the BlockingQueue, the PrimOp calls packClosure again.
                            IF_PAR_DEBUG(packet,
                                    debugBelch("packing hit a %s at %p, no TSO given (returning).\n",
                                        info_type_by_ip(info), closure));
                        return P_BLACKHOLE;

                    default: // an indirection, pack the indirectee (jump back to start)
                        closure = indirectee;
                        // this is a new case of "UNWIND_IND", a blackhole which is indeed an
                        // indirection. Difficult to catch in UNWIND_IND, so jump back here.
                        goto loop;
                }
            }

        case MVAR_CLEAN:
        case MVAR_DIRTY:
        case TVAR:
            errorBelch("Pack: packing type %s (%p) not possible",
                    info_type_by_ip(info), closure);
            return P_CANNOTPACK;

        case ARR_WORDS:
            return PackArray(closure);

        case MUT_ARR_PTRS_CLEAN:
        case MUT_ARR_PTRS_DIRTY:
        case MUT_ARR_PTRS_FROZEN0:
        case MUT_ARR_PTRS_FROZEN:
            /* We use the same routine as for ARR_WORDS The implementation of
               (immutable!) arrays uses these closure types, as well as that
               of mutable arrays.  => perhaps impossible to find out from the
               RTS whether we should allow duplication of the array or not.
               */
            IF_PAR_DEBUG(packet,
                    debugBelch("Packing pointer array @ %p!", closure));
            return PackArray(closure);

        case MUT_VAR_CLEAN:
        case MUT_VAR_DIRTY: // these guys are known as IORefs in the Haskell world
            errorBelch("Pack: packing type %s (%p) not possible",
                    info_type_by_ip(info),closure);
            return P_CANNOTPACK;

        case WEAK:
            goto unsupported;

        case PRIM: // Prim type holds internal immutable closures: MSG_TRY_WAKEUP,
                   // MSG_THROWTO, MSG_BLACKHOLE, MSG_NULL, MVAR_TSO_QUEUE
        case MUT_PRIM: // Mut.Prim type holds internal mutable closures:
                       // TVAR_WATCH_Q, ATOMIC_INVARIANT, INVARIANT_CHECK_Q,
                       // TREC_HEADER
        case TSO: // this might actually happen if the user is smart and brave
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
#ifdef THREADED_RTS
            // closure is spin-locked, loop back and spin until changed. Take the big
            // round to avoid compiler optimisations getting into the way
            write_barrier();
            goto loop;
#else
            // something's very wrong
            barf("Pack: found WHITEHOLE while packing");
#endif

        /* case SMALL_MUT_ARR_PTRS_CLEAN: */
        /* case SMALL_MUT_ARR_PTRS_DIRTY: */
        /* case SMALL_MUT_ARR_PTRS_FROZEN: */
        /* case SMALL_MUT_ARR_PTRS_FROZEN0: */
            /* unlike the standard arrays, small arrays do not have a card table.
             * Layout is thus: +------------------------------+
             *                 | hdr | #ptrs | payload (ptrs) |
             *                 +------------------------------+
             * No problem with using PackGeneric and vhs=1 in get_closure_info. */
            /* return PackGeneric(closure); */

unsupported:
            errorBelch("Pack: packing type %s (%p) not implemented",
                    info_type_by_ip(info), closure);
            return P_UNSUPPORTED;

impossible:
            errorBelch("{Pack}Daq Qagh: found %s (%p) when packing",
                    info_type_by_ip(info), closure);
            return P_IMPOSSIBLE;

        default:
            barf("Pack: strange closure %d", (nat)(info->type));
    } /* switch */
}

static StgWord PackGeneric(StgClosure* closure)
{
    nat size, ptrs, nonptrs, vhs, i;
    StgWord tag=0;

    /* store tag separately, pack with info ptr. */
    tag = GET_CLOSURE_TAG(closure);
    closure = UNTAG_CLOSURE(closure);

    /* get info about basic layout of the closure */
    get_closure_info(closure, NULL, &size, &ptrs, &nonptrs, &vhs);

    ASSERT(!IsBlackhole(closure));

    IF_PAR_DEBUG(packet,
            debugBelch("*>== %p (%s): generic packing"
                "(size=%d, ptrs=%d, nonptrs=%d, and tag %d)\n",
                closure, info_type(closure), size, ptrs, nonptrs,
                (int)tag));

    /* make sure we can pack this closure into the current buffer
       (otherwise this routine sends a message and resets the buffer) */
    if (!RoomToPack(HEADERSIZE + vhs + nonptrs)) return P_NOBUFFER;

    /* Record the location of the GA */
    RegisterOffset(closure);

    /* Allocate a GA for this closure and put it into the buffer
       Checks for globalisation scheme; default: globalise everything thunks
       if ( RtsFlags.ParFlags.globalising == 0 ||
       (closure_THUNK(closure) && !closure_UNPOINTED(closure)) )
       GlobaliseAndPackGA(closure);
       else
       */
    {
        Pack((StgWord) CLOSURE);  // marker for unglobalised closure
    }

    /* At last! A closure we can actually pack! */

    /*
       Remember, the generic closure layout is as follows:
       +-------------------------------------------------+
       | FIXED HEADER | VARIABLE HEADER | PTRS | NON-PRS |
       +-------------------------------------------------+

       The first (and actually only, in the default system) word of
       the header is the info pointer, tagging and offset apply.
       */
    /* pack fixed and variable header */
    // First word (==infopointer) is tagged and offset using macros above
    Pack((StgWord) (P_OFFSET(TAG_CLOSURE(tag, (StgClosure*) *((StgPtr) closure )))));
    // pack the rest of the header (variable header)
    for (i = 1; i < HEADERSIZE + vhs; ++i) {
        Pack((StgWord)*(((StgPtr)closure)+i));
    }

    /* register all ptrs for further packing */
    for (i = 0; i < ptrs; ++i) {
        QueueClosure(((StgClosure *) *(((StgPtr)closure)+(HEADERSIZE+vhs)+i)));
    }

    /* pack non-ptrs */
    for (i = 0; i < nonptrs; ++i) {
        Pack((StgWord)*(((StgPtr)closure)+(HEADERSIZE+vhs)+ptrs+i));
    }

    ASSERT(HEADERSIZE+vhs+ptrs+nonptrs==size); // no slop in closure, all packed

    unpacked_size += size;

    /* Record that this is a revertable black hole so that we can fill in
       its address from the fetch reply.  Problem: unshared thunks may cause
       space leaks this way, their GAs should be deallocated following an
       ACK.
       */

    /* future GUM code: convert to RBH
       if (closure_THUNK(closure) && !closure_UNPOINTED(closure)) {

       StgClosure *rbh;
       rbh = convertToRBH(closure);
       ASSERT(size>=HEADERSIZE+MIN_UPD_SIZE); // min size for any updatable closure
       ASSERT(rbh == closure);         // rbh at the same position (minced version)

    // record the thunk that has been packed so that we may abort and revert
    if (thunks_packed < MAX_THUNKS_PER_PACKET)
    thunks[thunks_packed++] = closure;
    // otherwise: abort packing right now (should not happen at all).
    }
    */

    return P_SUCCESS;

}

/* Packing PAPs: a PAP (partial application) represents a function
 * which has been given too few arguments for complete evaluation
 * (thereby defining a new function with fewer arguments).  PAPs are
 * packed by packing the function and the argument stack, where both
 * can point to static or dynamic (heap-allocated) closures which must
 * be packed later, and enqueued here.
 */
static StgWord PackPAP(StgPAP *pap)
{
    nat i;
    nat hsize;         // StgHeader size
    StgPtr p;          // stack object currently packed...
    nat args;          // no. of arguments
    StgWord bitmap;    // ptr indicator
    nat size;          // size of bitmap
    StgFunInfoTable *funInfo; // where to get bitmap...

    // JB 08/2007: remove, and store the tag separately...
    StgWord tag = 0;

    tag = GET_CLOSURE_TAG((StgClosure*) pap);
    pap = (StgPAP*) UNTAG_CLOSURE((StgClosure*) pap);

    ASSERT(LOOKS_LIKE_CLOSURE_PTR(pap));

    ASSERT(get_itbl((StgClosure*)pap)->type == PAP ||
            get_itbl((StgClosure*)pap)->type == AP);

    /* PAP/AP closure layout in GHC-6.x (see Closures.h):
     * +--------------------------------------------------------------+
     * | Header | (arity | n_args) | Function | Stack.|Stack.|Stack...|
     * +--------------------------------------------------------------+
     *   particularities:
     *   ----------------
     * PAP     : as described, normal Header
     * AP      : Thunk Header (1 extra word, see Closures.h)
     *
     * In previous versions, we treated AP_STACK in the same way. This
     * is wrong, AP_STACK closures can contain update frames and other
     * stack-only objects.
     */

    switch (get_itbl((StgClosure*)pap)->type) {
        case PAP:
            size  = pap_sizeW(pap);
            args  = pap->n_args;
            hsize = HEADERSIZE+1;
            break;

        case AP:
            size  = ap_sizeW((StgAP *)pap);
            args  = ((StgAP*) pap)->n_args;
            hsize = sizeofW(StgThunkHeader)+1;
            break;

        default:
            barf("PackPAP: strange info pointer, type %d ",
                    get_itbl((StgClosure*)pap)->type);
    }
    IF_PAR_DEBUG(packet,
            debugBelch("Packing Closure with stack (%s) @ %p,"
                "stack size %d\n",
                info_type((StgClosure*) pap), pap, args));

    // preliminaries: register closure, check for enough space...
    RegisterOffset((StgClosure*) pap);
    if (!RoomToPack(size - 1 + args)) // double stack size, but no fct field
        return P_NOBUFFER;

    // pack closure marker
    Pack((StgWord) CLOSURE);

    /*
     * first pack the header, which starts with non-ptrs:
     *   { StgHeader , (arity,args) }
     * StgHeader is where AP (ThunkHeader) differs from
     * PAP. Besides, both have one extra StgWord containing
     * (arity|n_args) resp. size. hsize got adjusted above, now pack
     * exactly this amount of StgWords.
     *
     * Store tag in first word of header (==infopointer) and offset it.
     */
    Pack((StgWord) (P_OFFSET(TAG_CLOSURE(tag, (StgClosure*) *((StgPtr) pap )))));
    for(i = 1; i < hsize; i++) {
        Pack((StgWord) *(((StgWord*)pap)+i));
    }

    /* Next, we find the function, which might be heap-allocated.
     * Instead of checking if it is heap_alloc.ed, we always queue it
     * and pack it later, unpacking is the reverse (needs to treat the
     * field as a pointer arg.).
     * Queue fun closure with tags, analyse without them...
     */
    funInfo = get_fun_itbl(UNTAG_CLOSURE(pap->fun));
    QueueClosure(pap->fun);

    unpacked_size += pap_sizeW(pap); // minimum: unpack only the PAP

    /* Now the payload, which is the argument stack of the function. A
     * packed bitmap inside the function closure indicates which stack
     * addresses are pointers(0 for ptr).  See GC:scavenge_PAP_payload()
     * and InfoTables.h,Constants.h for details...
     *
     * Saving the bitmap in the packet does not work easily, since it
     * can vary in size (bitmap/large bitmap). So we tag every stack
     * element with its type for unpacking (tags: PLC/CLOSURE), doubling
     * the stack size. We could pack only the tag for pointers, but then
     * we do not know the size in the buffer, so we pack the tag twice
     * for a pointer.
     *
     * Unpacking a PAP is the counterpart, which
     * creates an extra indirection for every pointer on the stack and
     * puts the indirection on the stack.
     *
     * TODO we should just pack the bitmap instead of doing all this
     * again when unpacking. TODO also handle large bitmap case (needs
     * offset, as we will be packing another pointer).
     */
    switch (funInfo->f.fun_type) {

        // these two use a large bitmap.
        case ARG_GEN_BIG:
        case ARG_BCO:
            errorBelch("PackPAP at %p: packing large bitmap, not implemented", pap);
            return P_UNSUPPORTED;
            // should be sort of:
            //  packLargeBitmapStack(pap->payload,GET_FUN_LARGE_BITMAP(funInfo),args);
            //  return;
            break;

        // another clever solution: fields in info table different for
        // some cases... and referring to autogenerated constants (Apply.h)
        case ARG_GEN:
            bitmap = funInfo->f.b.bitmap;
            break;

        default:
            bitmap = stg_arg_bitmaps[funInfo->f.fun_type];
    }

    p = (StgPtr) pap->payload;  // points to first stack element
    args = pap->n_args;// tells how many slots we find on the stack

    // extract bits and  size from bitmap (see Constants.h):
    size = BITMAP_SIZE(bitmap);
    // ASSERT(size == args); NO, not always n_args == BITMAP_SIZE(bitmap).
    // size refers to the bitmap for the whole function.
    bitmap = BITMAP_BITS(bitmap);

    IF_PAR_DEBUG(packet,
            debugBelch("Packing stack chunk, size %d (PAP.n_args=%d), bitmap %#o\n",
                size, (int)args, (nat)bitmap));

    /* now go through the small bitmap (its size should be == args???)
     * The bitmap contains 5/6 bit size, which should AFAICT be
     * identical to args.  Not ones, but zeros indicate pointers on the

     * stack! According to Scav.c, the "direction" of the bitmap traversal
     * is least to most significant ( bitmap>>1 corresponds to p++).
     */
    size = 0;
    while (args > 0) {
        if ((bitmap & 1) == 0) { /* Zero: a pointer*/
            ASSERT(LOOKS_LIKE_CLOSURE_PTR((StgClosure*) *p));
            Pack((StgWord) CLOSURE);         // pointer tag
            Pack((StgWord) CLOSURE);         // padding for constant size...
            QueueClosure((StgClosure*) *p);  // closure will be packed later
            unpacked_size += sizeofW(StgInd);// unpacking creates add. IND
            size++;
        } else {
            Pack((StgWord) PLC); // constant tag
            Pack((StgWord) *p); // and the argument
        }
        p++;                // advance in payload, next bit, next arg
        bitmap = bitmap>>1;
        args--;
    }

    /* PAP/AP in the pack buffer:
     * +-----------------------------------------------------------------+
     * | Header | (arity | n_args) | Tag | Arg/Tag | Tag | Arg/Tag | ... |
     * +-----------------------------------------------------------------+
     * Header can be 1 (normal) or 2 StgWords (Thunk Header)
     */

    IF_PAR_DEBUG(packet,
            debugBelch("packed PAP, stack contained %d pointers\n",
                size));
    return P_SUCCESS;
}

/* Packing Arrays.
 *
 * An Array in the heap can contain StgWords or Pointers (to
 * closures), and is thus of type StgArrWords or StgMutArrPtrs.
 *
 *     Array layout in heap/buffer is the following:
 * +----------------------------------------------------------------------+
 * | Header | size(in bytes) | word1 | word2 | ... | word_(size*wordsize) |
 * +----------------------------------------------------------------------+
 *
 * Packing ArrWords means to just pack all the words (as non-ptrs).
 * The array size is stored in bytes, but will always be word-aligned.
 *
 * MutArrPtrs (MUT_ARRAY_PTRS_* types) contain pointers to other
 * closures instead of words.
 * Packing MutArrPtrs means to enqueue/pack all pointers found.
 * OTOH, packing=copying a mutable array is not a good idea at all.
 * We implement it even though, leave it to higher levels to restrict.
 *
 */
static StgWord PackArray(StgClosure *closure)
{
    StgInfoTable *info;
    nat i, payloadsize, packsize;

    /* remove tag, store it in infopointer (same as above) */
    StgWord tag=0;

    tag = GET_CLOSURE_TAG(closure);
    closure = UNTAG_CLOSURE(closure);

    /* get info about basic layout of the closure */
    info = get_itbl(closure);

    ASSERT(info->type == ARR_WORDS
            || info->type == MUT_ARR_PTRS_CLEAN
            || info->type == MUT_ARR_PTRS_DIRTY
            || info->type == MUT_ARR_PTRS_FROZEN0
            || info->type == MUT_ARR_PTRS_FROZEN);

    if (info->type == ARR_WORDS) {
        payloadsize = arr_words_words((StgArrWords *)closure);
        packsize = payloadsize + HEADERSIZE + 1; // words in buffer
    } else {
        // MUT_ARR_PTRS_* {HDR,(no. of)ptrs,size(total incl.card table)}
        // Only pack header, not card table which follows the data.
        packsize = HEADERSIZE + 2;
        payloadsize = ((StgMutArrPtrs *)closure)->ptrs;
    }

    // the function in ClosureMacros.h would include the header:
    // arr_words_sizeW(stgCast(StgArrWords*,q));
    IF_PAR_DEBUG(pack,
            debugBelch("*>== %p (%s): packing array"
                "(%d words) (size=%d)\n",
                closure, info_type(closure), payloadsize,
                (int)closure_sizeW(closure)));

    /* TODO: make enough room in the pack buffer, see PackPAP code */
    if (!RoomToPack(packsize)) return P_NOBUFFER;

    /* record offset of the closure */
    RegisterOffset(closure);

    /* future GUM code:
       Check for globalisation scheme; default: globalise every thunk
    if ( RtsFlags.ParFlags.globalising == 0 ||
         (closure_THUNK(closure) && !closure_UNPOINTED(closure)) )
      GlobaliseAndPackGA(closure);
    else
    */
    Pack((StgWord) CLOSURE);  // marker for unglobalised closure

    /* Pack the header and the number of bytes/ptrs that follow)
       First word (info pointer) is tagged and offset */
    Pack((StgWord) (P_OFFSET(TAG_CLOSURE(tag, (StgClosure*) *((StgPtr) closure )))));
    /* pack the rest of the header (variable header) */
    for (i = 1; i < HEADERSIZE; ++i)
        Pack((StgWord)*(((StgPtr)closure)+i));

    if (info->type == ARR_WORDS) {
        /* pack no. of bytes to follow */
        Pack((StgWord) ((StgArrWords *)closure)->bytes);
        /* pack payload of the closure (all non-ptrs) */
        memcpy((globalPackBuffer->buffer) + pack_locn,
                ((StgArrWords *)closure)->payload,
                payloadsize * sizeof(StgWord));
        pack_locn += payloadsize;
    } else {
        /* MUT_ARR_PTRS_*: pack no. of ptrs and total size, enqueue pointers */
        Pack((StgWord) ((StgMutArrPtrs *)closure)->ptrs);
        Pack((StgWord) ((StgMutArrPtrs*)closure)->size);
        for (i=0; i<payloadsize; i++)
            QueueClosure(((StgMutArrPtrs *) closure)->payload[i]);
    }
    // this should be correct for both ARR_WORDS and MUT_ARR_*
    unpacked_size += closure_sizeW(closure);

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

StgClosure* UnpackGraph(pmPackBuffer *packBuffer,
                        STG_UNUSED Port inPort, Capability* cap)
{

    StgClosure *graphroot;

    IF_DEBUG(sanity, // do a sanity check on the incoming packet
            checkPacket(packBuffer));

#if !defined(PARALLEL_RTS)
    IF_DEBUG(scheduler,
            debugBelch("Packing: Header unpacked. (bufsize=%" FMT_Word
                ", heapsize=%" FMT_Word ")\nUnpacking closures now...\n",
                packBuffer->size, packBuffer->unpacked_size));
#else
    IF_PAR_DEBUG(pack,
            debugBelch("Packing: Header unpacked. (bufsize=%" FMT_Word
                ", heapsize=%" FMT_Word ")\nUnpacking closures now...\n",
                packBuffer->size, packBuffer->unpacked_size));
#endif

    graphroot = UnpackGraph_(packBuffer->buffer, packBuffer->size, cap);

    // if we hit this case, we are outside the deserialisation code.
    // Therefore: complain and abort the program.
    if (graphroot == NULL) {
        barf("Failure during unpacking, aborting program");
    }

    // wipe the pack buffer if we do sanity checks.
    // This should _NOT_ be done when using Haskell serialised structures!
    IF_DEBUG(sanity,
            {
                StgPtr p;
                for (p=(StgPtr)packBuffer->buffer; p<(StgPtr)(packBuffer->buffer)+(packBuffer->size); )
                    *p++ = 0xdeadbeef;
            });

    return (graphroot);
}

// Internal worker function, not allowed to edit the buffer at all
// (used with with an immutable Haskell ByteArray# as buffer for
// deserialisation). This function returns NULL upon
// errors/inconsistencies in buffer (avoiding to abort the program).
StgClosure* UnpackGraph_(StgWord *buffer, StgInt size, Capability* cap)
{
    StgWord* bufptr;
    StgClosure *closure, *parent, *graphroot;
    nat pptr = 0, pptrs = 0, pvhs = 0;
    nat currentOffset;

    IF_PAR_DEBUG(packet, debugBelch("Unpacking buffer @ %p, size %" FMT_Word,
                buffer, size));

    // Initialisation: alloc. hash table and queue, take lock
    InitPacking(rtsTrue);

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
            currentOffset = ((nat) (bufptr - buffer)) + offsetpadding;
            // ...which is at least 1!
        }

        /* Unpack one closure (or offset or PLC). This allocates heap
         * space, checks for PLC/offset etc. The returned pointer is
         * tagged with the tag found at sender side.
         */
        closure = UnpackClosure (/*in/out*/&bufptr, cap);

        if (closure == NULL) {
            // something is wrong with the packet, give up immediately
            // we do not try to find out details of what is wrong...
#if !defined(PARALLEL_RTS)
            IF_DEBUG(scheduler, debugBelch("Unpacking error at address %p",bufptr));
#else
            IF_PAR_DEBUG(pack, debugBelch("Unpacking error at address %p",bufptr));
#endif
            DonePacking();
            return (StgClosure *) NULL;
        }

        // store closure address for offsets (if we should, see above)
        if (currentOffset != 0) {
            IF_PAR_DEBUG(packet,
                    debugBelch("---> Entry in Offset Table: (%d, %p)\n",
                        currentOffset, closure));
            // note that the offset is stored WITH TAG
            insertHashTable(offsetTable, currentOffset, (void*) closure);
        }

        /*
         * Set the pointer in the parent to point to chosen closure. If
         * we're at the top of the graph (our parent is NULL), then we
         * want to return this closure to our caller.
         */
        if (parent == NULL) {
            /* we are at the root. Do not remove the tag */
            graphroot = closure;
#if !defined(PARALLEL_RTS)
            IF_DEBUG(scheduler, debugBelch("Graph root %p, tag %x", closure,
                        (int) GET_CLOSURE_TAG(closure)));
#else
            IF_PAR_DEBUG(pack,
                    debugBelch("Graph root %p, tag %x", closure,
                        (int) GET_CLOSURE_TAG(closure)));
#endif
        } else {
            // packet fragmentation code would need to check whether
            // there is a temporary blackhole here. Not supported for now.

            // write ptr to new closure into parent at current position (pptr)
            ((StgPtr) parent)[HEADERSIZE + pvhs + pptr] = (StgWord) closure;
        }

        // Locate next parent pointer (incr ppr, dequeue next closure when at end)
        LocateNextParent(&parent, &pptr, &pptrs, &pvhs);

        // stop when buffer size has been reached or end of graph
    } while ((parent != NULL) && (size > (bufptr-buffer)));

    if (parent != NULL) {
        // this case is valid when one graph can stretch across several
        // packets (fragmentation), in which case we would save the state.

        errorBelch("Graph fragmentation not supported, packet full");
        return (StgClosure *) NULL;
    }

    DonePacking();

    // assertions on buffer
    // magic end-of-buffer word is present:
    IF_DEBUG(sanity, ASSERT(*(bufptr++) == END_OF_BUFFER_MARKER));

    // we unpacked exactly as many words as there are in the buffer
    ASSERT(size == (nat) (bufptr-buffer));

    // ToDo: are we *certain* graphroot has been set??? WDP 95/07
    ASSERT(graphroot!=NULL);

#if !defined(PARALLEL_RTS)
    IF_DEBUG(scheduler,
            GraphFingerPrint(graphroot);
            debugBelch(">>> Fingerprint of unpacked graph rooted at %p:\n"
                "\t{%s}\n", graphroot, fingerPrintStr));
#else
    IF_PAR_DEBUG(pack,
            GraphFingerPrint(graphroot);
            debugBelch(">>> Fingerprint of unpacked graph rooted at %p\n"
                "\t{%s}\n", graphroot, fingerPrintStr));
#endif

    return graphroot;
}

/*
  Find the next pointer field in the parent closure, retrieve information
  about its variable header size and no. of pointers.
  If the current parent has been completely unpacked already, get the
  next closure from the global closure queue, and register the new variable
  header size and no. of pointers.
  Example situation:

*parentP
    |
    V
  +--------------------------------------------------------------------+
  |hdr| variable hdr  | ptr1 | ptr2 | ptr3 | ... | ptrN | non-pointers |
  +--------------------------------------------------------------------+
      <--- *vhsP=2 --->                A
                                       |
	 *pptrs = N                 *pptr=3
*/
//@cindex LocateNextParent
STATIC_INLINE void LocateNextParent(parentP, pptrP, pptrsP, pvhsP)
StgClosure **parentP;
nat *pptrP, *pptrsP, *pvhsP;
{
    nat size, nonptrs;

    /* pptr as an index into the current parent; find the next pointer field
       in the parent by increasing pptr; if that takes us off the closure
       (i.e. *pptr + 1 > *pptrs) grab a new parent from the closure queue
       */
    (*pptrP)++;
    while (*pptrP + 1 > *pptrsP) {
        /* *parentP has been constructed (all pointer set); so check it now */

        IF_DEBUG(sanity,
                if (*parentP != (StgClosure*)NULL) // not root
                checkClosure(*parentP));

        *parentP = DeQueueClosure();

        if (*parentP == NULL) {
            break;
        } else {
            get_closure_info(*parentP, NULL, &size, pptrsP, &nonptrs, pvhsP);
            *pptrP = 0;
        }
    }
    /* *parentP points to the new (or old) parent; */
    /* *pptr, *vhsP, and *pptrs have been updated referring to the new parent */
}


/*
   UnpackClosure is the heart of the unpacking routine. It is called for
   every closure found in the packBuffer.
   UnpackClosure does the following:
     - check for the kind of the closure (PLC, Offset, std closure)
     - copy the contents of the closure from the buffer into the heap
     - update LAGA tables (in particular if we end up with 2 closures
       having the same GA, we make one an indirection to the other)
     - set the GAGA map in order to send back an ACK message
  In case of any unexpected data, the routine returns NULL.

   At the end of this function,
   *bufptrP points to the next word in the pack buffer to be unpacked.

   "pointer tagging":
  When unpacking, UnpackClosure() we add the tag to its return value,
  but enqueue the closure address WITHOUT A TAG, so we can access the
  unpacked closure directly by the enqueued pointer.
  The closure WITH TAG is saved as offset value in the offset hash
  table (key=offset, value=address WITH TAG), to be filled in other
  closures as a pointer field.
  When packing, we did the reverse: saved the closure address WITH TAG
  in the queue, but stored it WITHOUT TAG in the offset table (as a
  key, value was the offset).
*/

static  StgClosure*
UnpackClosure (StgWord **bufptrP, Capability* cap)
{
    StgClosure *closure;
    nat size,ptrs,nonptrs,vhs,i;
    StgInfoTable *ip;
    StgWord tag = 0;

    /* Unpack the closure body, if there is one; three cases:
       - PLC: closure is just a pointer to a static closure
       - Offset: closure has been unpacked already
       - else: copy data from packet into closure
       */
    switch ((StgWord) **bufptrP) {
        // these two cases respect the "pointer tags" by
        // design: either the tag was not removed at all (PLC case), or
        // the offset refers to an already unpacked (=> tagged) closure.
        case PLC:
            closure = UnpackPLC(bufptrP);
            break;
        case OFFSET:
            closure = UnpackOffset(bufptrP);
            break;
        case CLOSURE:

            (*bufptrP)++; // skip marker

            /* The first word of a closure is the info pointer. In contrast,
               in the packet (where (*bufptrP) points to a packed closure),
               the first word is an info _offset_, which additionally was
               tagged before packing. We remove and store the tag added to the
               info offset, and compute the untagged info table pointer from
               the info offset.
               The original value is overwritten inside the buffer.
               */
            tag = GET_CLOSURE_TAG((StgClosure*) **bufptrP);
            ip  = UNTAG_CAST(StgInfoTable*, P_POINTER(**bufptrP));
            IF_PAR_DEBUG(packet,
                    debugBelch("pointer tagging: removed tag %d "
                        "from info pointer %p in packet\n",
                        (int) tag, ip));

            /* *************************************************************
               The essential part starts here: allocate heap space, fill in
               the closure and queue it to get the pointer payload filled
               later.
               */
            if (!LOOKS_LIKE_INFO_PTR((StgWord) ip)) {
                errorBelch("Invalid info pointer in packet");
                return (StgClosure *) NULL;
            }

            /*
             * Close your eyes.  You don't want to see where we're
             * looking. You can't get closure info until you've unpacked the
             * variable header, but you don't know how big it is until you've
             * got closure info.  So...we trust that the closure in the buffer
             * is organized the same way as they will be in the heap...at
             * least up through the end of the variable header.
             */

            get_closure_info((StgClosure *) *bufptrP, INFO_PTR_TO_STRUCT(ip),
                    &size, &ptrs, &nonptrs, &vhs);

            switch (INFO_PTR_TO_STRUCT(ip)->type) {
                // branch into new special routines:
                case PAP:
                case AP:
                    closure = UnpackPAP(ip, bufptrP, cap);
                    /* see below, might create/enQ some INDs */
                    break;

                case ARR_WORDS:
                case MUT_ARR_PTRS_CLEAN:
                case MUT_ARR_PTRS_DIRTY:
                case MUT_ARR_PTRS_FROZEN0:
                case MUT_ARR_PTRS_FROZEN:
                    // for MUT_ARR*, enqueues the closure, needs to alloc. card
                    // table space after data space.
                    closure = UnpackArray(ip, bufptrP, cap);
                    break;

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
                /* case SMALL_MUT_ARR_PTRS_CLEAN: */
                /* case SMALL_MUT_ARR_PTRS_DIRTY: */
                /* case SMALL_MUT_ARR_PTRS_FROZEN0: */
                /* case SMALL_MUT_ARR_PTRS_FROZEN: */

                    IF_PAR_DEBUG(packet,
                            debugBelch("Allocating %d heap words for %s-closure:\n"
                                "(%d ptrs, %d non-ptrs, vhs = %d)\n"
                                , size, info_type_by_ip(INFO_PTR_TO_STRUCT(ip)),
                                ptrs, nonptrs, vhs));

                    closure = (StgClosure*) allocate(cap, size);

                    /*
                       Remember, the generic closure layout is as follows:
                       +------------------------------------------------+
                       | IP | FIXED HDR | VARIABLE HDR | PTRS | NON-PRS |
                       +------------------------------------------------+
                       Note that info ptr (IP) is assumed to be first hdr. field
                       */
                    /* Fill in the info pointer (extracted before) */
                    ((StgPtr)closure)[0] = (StgWord) ip;
                    (*bufptrP)++;

                    /* Fill in the rest of the fixed header (if any) */
                    for (i = 1; i < HEADERSIZE; i++)
                        ((StgPtr)closure)[i] = *(*bufptrP)++;

                    /* Fill in the packed variable header */
                    for (i = 0; i < vhs; i++)
                        ((StgPtr)closure)[HEADERSIZE + i] = *(*bufptrP)++;

                    /* Pointers will be filled in later, but set zero here to
                       easily check if there is a temporary BH.
                       */
                    for (i = 0; i < ptrs; i++)
                        ((StgPtr)closure)[HEADERSIZE + vhs + i] = 0;

                    /* Fill in the packed non-pointers */
                    for (i = 0; i < nonptrs; i++)
                        ((StgPtr)closure)[HEADERSIZE + i + vhs + ptrs]
                            =  *(*bufptrP)++;

                    ASSERT(HEADERSIZE+vhs+ptrs+nonptrs == size);

                    QueueClosure(closure);
                    break;

                    // other cases are unsupported/unexpected, and caught here
                default:
                    errorBelch("Found unexpected closure type (%x) in packet when unpacking",
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

// handling special case of PAPs. Returns NULL in case of errors
static StgClosure * UnpackPAP(StgInfoTable *info, StgWord **bufptrP, Capability* cap)
{
    StgPAP *pap;
    nat args, size, hsize, i;

#if defined(DEBUG)
    // for "manual sanity check" with debug flag "packet" only
    StgWord bitmap;
    bitmap = 0;
#endif

    /* Unpacking a PAP/AP
     *
     * in the buffer:
     * +------------------------------------------------------------------+
     * | IP'| Hdr | (arity , n_args) | Tag | Arg/Tag | Tag | Arg/Tag | ...|
     * +------------------------------------------------------------------+
     *
     * Header size is 1 (normal) for PAP or 2 StgWords (Thunk Header) for AP.
     * IP' is the offset of IP from the known constant (see P_POINTER).
     * Tag is PLC or CLOSURE constant, repeated if it is CLOSURE,
     * followed by the arg otherwise. Unpacking creates indirections and
     * inserts references to them when tag CLOSURE is found, otherwise
     * just unpacks the arg.
     *
     * should give in the heap:
     * +-----------------------------------------------------------------+
     * | IP | Hdr | (arity , n_args) | Fct. | Arg/&Ind1 | Arg/&Ind2 | ...|
     * +-----------------------------------------------------------------+
     * followed by <= n_args indirections pointed at from the stack
     */

    /* calc./alloc. needed closure space in the heap.
     * We are using the common macros provided.
     */
    switch (INFO_PTR_TO_STRUCT(info)->type) {
        case PAP:
            hsize = HEADERSIZE + 1;
            args  = ((StgPAP*) *bufptrP)->n_args;
            size  = PAP_sizeW(args);
            break;
        case AP:
            hsize = sizeofW(StgThunkHeader) + 1;
            args  = ((StgAP*)  *bufptrP)->n_args;
            size  = AP_sizeW(args);
            break;
            /*
               case AP_STACK:
               hsize = sizeofW(StgThunkHeader)+1;
               args  = ((StgAP_STACK*) *bufptrP)->size;
               size  = AP_STACK_sizeW(args);
               break;
               */
        default:
            errorBelch("UnpackPAP: strange info pointer, type %d ",
                    INFO_PTR_TO_STRUCT(info)->type);
            return (StgClosure *) NULL;
    }
    IF_PAR_DEBUG(packet,
            debugBelch("allocating %d heap words for a PAP(%d args)\n",
                size, args));
    pap = (StgPAP *) allocate(cap, size);

    /* fill in info ptr (extracted and given as argument by caller) */
    ((StgPtr) pap)[0] = (StgWord) info;
    (*bufptrP)++;

    /* fill in sort-of-header fields (real header and (arity,n_args)) */
    for(i = 1; i < hsize; i++) {
        ((StgPtr) pap)[i] = (StgWord) *(*bufptrP)++;
    }
    // enqueue to get function field filled (see get_closure_info)
    QueueClosure((StgClosure*) pap);

    // zero out the function field (for BH check in UnpackGraph)
    pap->fun = (StgClosure*) 0;

    // unpack the stack (size == args), starting at pap[hsize]
    // make room for fct. pointer, thus start at hsize+1
    for (i = hsize+1; i < size; i++) {
        StgClosure* ind;
        switch ((long) **bufptrP) {
            case PLC:
                // skip marker, unpack data into stack
                (*bufptrP)++;
                ((StgPtr) pap)[i] = (StgWord) *(*bufptrP)++;
                IF_PAR_DEBUG(packet, bitmap |= 1); // set bit in bitmap
                break;
            case CLOSURE:
                // skip 2 markers, create/enqueue indirection, put it on the stack
                (*bufptrP)+=2;
                ind = (StgClosure*) allocate(cap, sizeofW(StgInd));
                SET_HDR(ind, &stg_IND_info, CCS_SYSTEM); // ccs to be checked...
                ((StgInd*)ind)->indirectee = 0; // for BH-check in UnpackGraph
                ((StgPtr) pap)[i] = (StgWord) ind;
                QueueClosure(ind);
                break;
            default:
                errorBelch("UnpackPAP: strange tag %d, should be %d or %d.",
                        (nat) **bufptrP, (nat) PLC, (nat) CLOSURE);
                return (StgClosure *) NULL;
        }
        IF_DEBUG(sanity, bitmap = bitmap << 1); // shift to next bit
    }

    IF_DEBUG(sanity,
            debugBelch("unpacked a %s @ address %p, "
                "%d args, constructed bitmap %#o.\n",
                info_type((StgClosure*) pap),pap,
                args, (int) bitmap));

    return (StgClosure*) pap;
}

// handling special case of arrays. Returns NULL in case of errors.
static StgClosure*
UnpackArray(StgInfoTable* info, StgWord **bufptrP, Capability* cap)
{
    nat size;
    StgMutArrPtrs *array; // can also be StgArrWords, but fields not
                          // used in this case.
    /*
     * We have to distinguish pointer arrays from word arrays. In the
     * case of pointers, we enqueue the unpacked closure for filling in
     * pointers, otherwise we just unpack the words (doing a memcpy).
     *
     * Since GHC-6.13, ptr arrays additionally carry a "card table" for
     * generational GC (to indicate mutable/dirty elements). For
     * unpacking, allocate the card table and fill it with zero.
     *
     * With this change, probably split into two methods??
     */

    switch (INFO_PTR_TO_STRUCT(info)->type) {
        case ARR_WORDS:
            /*     Array layout in heap/buffer is the following:
             * +-----------------------------------------------------------+
             * | Header |size(in bytes)| word1 | word2 | ... | word_(size) |
             * +-----------------------------------------------------------+
             *
             * additional size to allocate is the 2nd word in buffer (in bytes)
             * but we read it using the selector function in ClosureMacros.h
             */
            size = sizeofW(StgArrWords) + arr_words_words((StgArrWords*) *bufptrP);
            IF_PAR_DEBUG(packet,
                    debugBelch("Unpacking word array, size %d\n", size));
            array = (StgMutArrPtrs *) allocate(cap, size);

            /* copy header and payload words in one go */
            memcpy(array, *bufptrP, size*sizeof(StgWord));
            /* correct first word (info ptr, stored with offset in packet) */
            ((StgPtr)array)[0] = (StgWord) info;

            *bufptrP += size; // advance buffer pointer, and done
            break;

        case MUT_ARR_PTRS_CLEAN:
        case MUT_ARR_PTRS_DIRTY:
        case MUT_ARR_PTRS_FROZEN0:
        case MUT_ARR_PTRS_FROZEN:
            /* Array layout in buffer:
             * +------------------------+......................................+
             * | IP'| Hdr | ptrs | size | ptr1 | ptr2 | .. | ptrN | card space |
             * +------------------------+......................................+
             *                            (added in heap when unpacking)
             * ptrs indicates how many pointers to come (N). Size field gives
             * total size for pointers and card table behind (to add).
             */
            // size = sizeofW(StgMutArrPtrs) + (StgWord) *((*bufptrP)+2);
            size = closure_sizeW_((StgClosure*) *bufptrP, INFO_PTR_TO_STRUCT(info));
            ASSERT(size ==
                    sizeofW(StgMutArrPtrs) + ((StgMutArrPtrs*) *bufptrP)->size);
            IF_PAR_DEBUG(packet,
                    debugBelch("Unpacking ptrs array, %" FMT_Word
                        " ptrs, size %d\n",
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
            QueueClosure((StgClosure*)array);
            break;

        default:
            errorBelch("UnpackArray: unexpected closure type %d",
                    INFO_PTR_TO_STRUCT(info)->type);
            return (StgClosure *) NULL;
    }

    IF_PAR_DEBUG(packet,
            debugBelch(" Array created @ %p.\n",array));

    return (StgClosure*) array;
}

//@cindex UnpackPLC
STATIC_INLINE  StgClosure *UnpackPLC(StgWord **bufptrP)
{
    StgClosure* plc;

    ASSERT((long) **bufptrP == PLC);

    (*bufptrP)++; // skip marker
    // Not much to unpack; just a static local address
    // but need to correct the offset
    plc = (StgClosure*) P_POINTER(**bufptrP);
    (*bufptrP)++; // skip address
    IF_PAR_DEBUG(packet,
            debugBelch("*<^^ Unpacked PLC at %p\n", plc));
    return plc;
}

//@cindex UnpackOffset
STATIC_INLINE  StgClosure *UnpackOffset(StgWord **bufptrP)
{
    StgClosure* existing;
    int offset;

    ASSERT((long) **bufptrP == OFFSET);

    (*bufptrP)++; // skip marker
    // unpack nat; find closure for this offset
    offset = (nat) **bufptrP;
    (*bufptrP)++; // skip offset

    // find this closure in an offset hashtable (we can have several packets)
    existing = (StgClosure *) lookupHashTable(offsetTable, offset);

    IF_PAR_DEBUG(packet,
            debugBelch("*<__ Unpacked indirection to closure %p (was OFFSET %d, current padding %d)",
                existing, offset, offsetpadding));

    // we should have found something...
    ASSERT(existing);

    return existing;
}


/* functions to save and restore the unpacking state from a
 * saved format (including queue and offset table).
 * Format is defined as type "UnpackInfo".

static
StgClosure* restoreUnpackState(UnpackInfo* unpack,StgClosure** graphroot,
		nat* pptr, nat* pptrs, nat* pvhs) {
  nat size, i;
  StgClosure* parent;

  IF_PAR_DEBUG(pack,
	       debugBelch("restore unpack state"));
  ASSERT(unpack != NULL);

  parent = unpack->parent;
  *pptr = unpack->pptr;
  *pptrs = unpack->pptrs;
  *pvhs = unpack->pvhs;
  *graphroot = unpack->graphroot;

  size = unpack->queue_length;
  for (i = 0; i < size; i++) // if no queue (size == 0): no action.
    QueueClosure(*(unpack->queue + i));

  // if we restore an unpack state, we use an existing hashtable:
  freeHashTable(offsetTable, NULL);
  offsetTable = (HashTable *) unpack->offsets;
  offsetpadding = unpack->offsetpadding;

  // free allocated memory:
  pmFree(unpack->queue);
  pmFree(unpack);

  IF_PAR_DEBUG(pack,
	       debugBelch("unpack state restored (graphroot: %p, current "
			  "parent: %p (ptr %d of %d, vhs= %d, offset %d).",
		     *graphroot, parent, *pptr, *pptrs, *pvhs, offsetpadding));
  return parent;
}

static
StgClosure** saveQueue(nat* size) {
  StgClosure** queue;

  // closures are saved as StgPtr in the queue...
  ASSERT(clq_pos <= clq_size); // we want to have a positive size
  *size = clq_size - clq_pos;

  if (*size == 0) return NULL; // no queue to save

  // queue to save:
  IF_PAR_DEBUG(packet,
	       debugBelch("saveQueue: saving ");
	       PrintClosureQueue());
  queue = (StgClosure **) pmMallocBytes(*size * sizeof(StgClosure*),
					 "saveQueue: Queue");
  memcpy(queue, ClosureQueue+clq_pos, *size * sizeof(StgClosure*));
  IF_PAR_DEBUG(packet,
	       { nat j;
	         debugBelch("saveQueue: saved this queue:\n");
		 for (j = 0; j < *size; j++)
		   debugBelch("\tClosure %d: %p\n",*size - j,queue[j]);
	       });
  return queue;
}

static
UnpackInfo* saveUnpackState(StgClosure* graphroot, StgClosure* parent,
			    nat pptr, nat pptrs, nat pvhs) {
  UnpackInfo* save;
  nat size;

  save = pmMallocBytes(sizeof(UnpackInfo),"saveUnpackState: UnpackInfo");
  IF_PAR_DEBUG(pack,
	       debugBelch("saving current unpack state at %p",save);
	       debugBelch("graphroot: %p, current parent: %p (ptr %d of %d, vhs= %d)",
		     graphroot, parent, pptr, pptrs, pvhs));
  // simple tasks: save numbers
  save->parent = parent;
  save->pptr = pptr;
  save->pptrs = pptrs;
  save->pvhs = pvhs;
  save->graphroot = graphroot;

  // complicated tasks: save queue and offset table
  save->queue = saveQueue(&size);
  save->queue_length = size;
  save->offsetpadding = offsetpadding; // padding for keys in offsetTable
  save->offsets = offsetTable;         // hashtable remains allocated

  IF_PAR_DEBUG(pack,
	       debugBelch("unpack state saved (offsetpadding %d in "
			  "hashtable at %p, %d closures in queue at %p).",
			  save->offsetpadding, save->offsets,
			  save->queue_length, save->queue));

  return save;
}
 */

/* Experimental feature: serialisation into a Haskell Byte Array and
 * respective deserialisation.
 *
 */

// pack, then copy the buffer into newly (Haskell-)allocated space
// (unless packing was blocked, in which case we return the error code)
// This implements primitive serialize# and #trySerialize (if tso==NULL).
StgClosure* tryPackToMemory(StgClosure* graphroot,
                            StgTSO* tso, Capability* cap)
{
    pmPackBuffer* buffer;
    StgArrWords* wordArray;

    ACQUIRE_LOCK(&pack_mutex);

    buffer = PackNearbyGraph(graphroot, tso);

    if (isPackError(buffer)) {
        // packing hit an error, return this error to caller
        RELEASE_LOCK(&pack_mutex);
#ifndef DEBUG
        // if we are not debugging, crash the system upon impossible cases.
        if (((StgWord) buffer) == P_IMPOSSIBLE) {
            barf("GHC RTS found an impossible closure during packing.");
            // never returns
        }
#endif
        return ((StgClosure*) buffer);
    }

    /* allocate space to hold an array (size is buffer size * wordsize)
       +---------+----------+------------------------+
       |ARR_WORDS| n_bytes  | data (array of words)  |
       +---------+----------+------------------------+
       */
    wordArray = (StgArrWords*) allocate(cap, 2 + buffer->size);
    SET_HDR(wordArray, &stg_ARR_WORDS_info, CCS_SYSTEM); // ccs to be checked!
    wordArray->bytes = sizeof(StgWord) * (buffer->size);
    memcpy((void*) &(wordArray->payload),
            (void*) (buffer->buffer),
            (buffer->size)*sizeof(StgWord));

    RELEASE_LOCK(&pack_mutex);

    return ((StgClosure*) wordArray);
}

// unpacking from a Haskell array (using the Haskell Byte Array)
// may return error code P_GARBLED
StgClosure* UnpackGraphWrapper(StgArrWords* packBufferArray, Capability* cap)
{
    nat size;
    StgWord *buffer;
    StgClosure* newGraph;

    // UnpackGraph uses global data structures and static functions
    ACQUIRE_LOCK(&pack_mutex);

    size = packBufferArray->bytes / sizeof(StgWord);
    buffer = (StgWord*) packBufferArray->payload;

    // unpack. Might return NULL in case the buffer was inconsistent.
    newGraph = UnpackGraph_(buffer, size, cap);

    RELEASE_LOCK(&pack_mutex);

    return (newGraph == NULL ? (StgClosure *) P_GARBLED : newGraph);
}


// debugging functions
#if defined(DEBUG)

/*
  Generate a finger-print for a graph.  A finger-print is a string,
  with each char representing one node; depth-first traversal.

  Will only be called inside this module. pack_mutex should be held.
*/

/* this array has to be kept in sync with includes/ClosureTypes.h */
#if !(N_CLOSURE_TYPES == 65 )
#error Wrong closure type count in fingerprint array. Check code.
#endif
static char* fingerPrintChar =
  "0ccccccCC"    /* INVALID CONSTRs (0-8) */
  "fffffff"      /* FUNs (9-15) */
  "ttttttt"      /* THUNKs (16-23) */
  "TBAPP___"     /* SELECTOR BCO AP PAP AP_STACK INDs (24-31) */
  "RRRRFFFF"     /* RETs FRAMEs (32-39) */
  "*@MMT"        /* BQ BLACKHOLE MVARs TVAR (40-43) */
  "aAAAAmmwppXS" /* ARRAYs MUT_VARs WEAK PRIM MUT_PRIM TSO STACK (44-55) */
  "&FFFWZZZZ"    /* TREC (STM-)FRAMEs WHITEHOLE SmallArr (56-64) */
  ;


// recursive worker function:
static void GraphFingerPrint_(StgClosure *p);

static void GraphFingerPrint(StgClosure *p)
{
    ASSERT(tmpClosureTable==NULL);

    // delete old fingerprint:
    fingerPrintStr[0]='\0';

    /* init hash table */
    tmpClosureTable = allocHashTable();

    /* now do the real work */
    GraphFingerPrint_(p);

    /* nuke hash table */
    freeHashTable(tmpClosureTable, NULL);
    tmpClosureTable = NULL;

    ASSERT(strlen(fingerPrintStr)<=MAX_FINGER_PRINT_LEN);
}

/*
  This is the actual worker functions.
  All recursive calls should be made to this function.
*/
static void GraphFingerPrint_(StgClosure *p)
{
    nat i, len, args, arity;
    const StgInfoTable *info;
    StgWord *payload;

    // first remove potential pointer tags
    p = UNTAG_CLOSURE(p);

    len = strlen(fingerPrintStr);
    ASSERT(len<=MAX_FINGER_PRINT_LEN);
    if (len+2 >= MAX_FINGER_PRINT_LEN)
        return;
    /* at most 7 chars added immediately (unchecked) for this node */
    if (len+7 >= MAX_FINGER_PRINT_LEN) {
        strcat(fingerPrintStr, "--end");
        return;
    }
    /* check whether we have met this node already to break cycles */
    if (lookupHashTable(tmpClosureTable, (StgWord)p)) { // ie. already touched
        strcat(fingerPrintStr, ".");
        return;
    }

    /* record that we are processing this closure */
    insertHashTable(tmpClosureTable, (StgWord) p, (void *)rtsTrue/*non-NULL*/);

    ASSERT(LOOKS_LIKE_CLOSURE_PTR(p));

    info = get_itbl((StgClosure *)p);

    // append char for this node
    fingerPrintStr[len] = fingerPrintChar[info->type];
    fingerPrintStr[len+1] = '\0';
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
            strcat(fingerPrintStr, "20(");
            // special treatment for thunks... extra smp header field
            GraphFingerPrint_(((StgThunk *)p)->payload[0]);
            GraphFingerPrint_(((StgThunk *)p)->payload[1]);
            if (strlen(fingerPrintStr)+2<MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case FUN_2_0:
        case CONSTR_2_0:
            // append char for this node
            strcat(fingerPrintStr, "20(");
            GraphFingerPrint_(((StgClosure *)p)->payload[0]);
            GraphFingerPrint_(((StgClosure *)p)->payload[1]);
            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case THUNK_1_0:
            // append char for this node
            strcat(fingerPrintStr, "10(");
            GraphFingerPrint_(((StgThunk *)p)->payload[0]);
            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case FUN_1_0:
        case CONSTR_1_0:
            // append char for this node
            strcat(fingerPrintStr, "10(");
            GraphFingerPrint_(((StgClosure *)p)->payload[0]);
            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case THUNK_0_1:
        case FUN_0_1:
        case CONSTR_0_1:
            // append char for this node
            strcat(fingerPrintStr, "01");
            break;

        case THUNK_0_2:
        case FUN_0_2:
        case CONSTR_0_2:
            // append char for this node
            strcat(fingerPrintStr, "02");
            break;

        case THUNK_1_1:
            // append char for this node
            strcat(fingerPrintStr, "11(");
            GraphFingerPrint_(((StgThunk *)p)->payload[0]);
            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case FUN_1_1:
        case CONSTR_1_1:
            // append char for this node
            strcat(fingerPrintStr, "11(");
            GraphFingerPrint_(((StgClosure *)p)->payload[0]);
            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                strcat(fingerPrintStr, ")");
            break;

        case THUNK:
            {
                char str[6];
                sprintf(str,"%d?(", info->layout.payload.ptrs);
                strcat(fingerPrintStr,str);
                for (i = 0; i < info->layout.payload.ptrs; i++)
                    GraphFingerPrint_(((StgThunk *)p)->payload[i]);
                if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fingerPrintStr, ")");
            }
            break;

        case FUN:
        case CONSTR:
            {
                char str[6];
                sprintf(str,"%d?(",info->layout.payload.ptrs);
                strcat(fingerPrintStr,str);
                for (i = 0; i < info->layout.payload.ptrs; i++)
                    GraphFingerPrint_(((StgClosure *)p)->payload[i]);
                if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fingerPrintStr, ")");
            }
            break;

        case THUNK_SELECTOR:
            GraphFingerPrint_(((StgSelector *)p)->selectee);
            break;

        case BCO:
            break;

        case AP_STACK:
            break;
            /*
            arity = ((StgAP_STACK*)p)->size;
            args  = arity;
            payload = (StgPtr) ((StgAP_STACK*)p)->payload;
            p = ((StgAP_STACK*)p)->fun;
            goto print;
            */
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
                strcat(fingerPrintStr, str);
                // follow the function, and everything on the stack
                GraphFingerPrint_((StgClosure *) (p));
                if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN) {
                    StgWord bitmap;
                    StgFunInfoTable *funInfo = get_fun_itbl(UNTAG_CLOSURE(p));
                    strcat(fingerPrintStr, "|");
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
                            GraphFingerPrint_((StgClosure *)(*payload));
                        else {
                            if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                                strcat(fingerPrintStr, "x");
                        }
                        payload++;
                        args--;
                        bitmap = bitmap>>1;
                    }
                }
                if (strlen(fingerPrintStr)+2 < MAX_FINGER_PRINT_LEN)
                    strcat(fingerPrintStr, ")");
            }
            break;

        case IND:
        case IND_PERM:
        case IND_STATIC:
            /* do not print the '_' for indirections */
            fingerPrintStr[len] = '\0';
            /* could also be different type StgIndStatic */
            GraphFingerPrint_(((StgInd*)p)->indirectee);
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
            // check if this is actually an indirection. See above in packing code
            // some Blackholes are actually indirections since ghc-7.0
            switch (((StgInfoTable*)get_itbl(UNTAG_CLOSURE(((StgInd*)p)->indirectee)))->type) {
                case TSO:
                case BLOCKING_QUEUE:
                    debugBelch("Woopsie! Found blackhole while doing fingerprint!\n");
                    break;
                default:
                    /* do not print the '_' for indirections */
                    fingerPrintStr[len] = '\0';
                    GraphFingerPrint_(((StgInd*)p)->indirectee);
                    break;
            }
            break;

        case MVAR_CLEAN:
        case MVAR_DIRTY:
            if (((StgMVar *)p)->value != &stg_END_TSO_QUEUE_closure)
                GraphFingerPrint_(((StgMVar *)p)->value);
            break;

        case TVAR:
            // The TVAR type subsumes both the var itself and a watch queue; the
            // latter holds a TSO or an "Atomic Invariant" where the former
            // (clean/dirty) holds the current value as its first payload. Anyways,
            // while useful for GC, the double meaning of the first payload is not
            // useful for fingerprinting. We do not descend into TVars.
            break;

        case ARR_WORDS:
            { // record size only (contains StgWords, not pointers)
                char str[6];
                sprintf(str, "%ld", (long) arr_words_words((StgArrWords*)p));
                strcat(fingerPrintStr, str);
            }
            break;

        case MUT_ARR_PTRS_CLEAN:
        case MUT_ARR_PTRS_DIRTY:
        case MUT_ARR_PTRS_FROZEN0:
        case MUT_ARR_PTRS_FROZEN:
            {
                char str[6];
                sprintf(str, "%ld", (long)((StgMutArrPtrs*)p)->ptrs);
                strcat(fingerPrintStr, str);
                nat i;
                for (i = 0; i < ((StgMutArrPtrs*)p)->ptrs; i++) {
                    //contains closures... follow
                    GraphFingerPrint_(((StgMutArrPtrs*)p)->payload[i]);
                }
                break;
            }
        case MUT_VAR_CLEAN:
        case MUT_VAR_DIRTY:
            GraphFingerPrint_(((StgMutVar *)p)->var);
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

#if 0
        case SMALL_MUT_ARR_PTRS_CLEAN:
        case SMALL_MUT_ARR_PTRS_DIRTY:
        case SMALL_MUT_ARR_PTRS_FROZEN0:
        case SMALL_MUT_ARR_PTRS_FROZEN:
            {
                char str[6];
                sprintf(str,"%ld",(long)((StgSmallMutArrPtrs*)p)->ptrs);
                strcat(fingerPrintStr,str); 
                nat i;
                for (i = 0; i < ((StgSmallMutArrPtrs*)p)->ptrs; i++) {
                    //contains closures... follow
                    GraphFingerPrint_(((StgSmallMutArrPtrs*)p)->payload[i]);
                }
                break;
            }
#endif

        default:
            barf("GraphFingerPrint_: unknown closure %d",
                    info -> type); // , info_type((StgClosure*) p)); // info_type_by_ip(info));
    }

}

/*  Doing a sanity check on a packet.
    This does a full iteration over the packet, as in UnpackGraph.
*/
void checkPacket(pmPackBuffer *packBuffer)
{
    StgInt packsize, openptrs;
    nat clsize, ptrs, nonptrs, vhs;
    StgWord *bufptr;
    HashTable *offsets;

    IF_PAR_DEBUG(pack, debugBelch("checking packet (@ %p) ...",
                packBuffer));

    offsets = allocHashTable(); // used to identify valid offsets
    packsize = 0; // compared against value stored in packet
    openptrs = 1; // counting pointers (but no need for a queue to fill them in)
    // initially, one pointer is open (graphroot)
    bufptr = packBuffer->buffer;

    do {
        StgWord tag;
        StgInfoTable *ip;

        IF_DEBUG(sanity, ASSERT(*bufptr != END_OF_BUFFER_MARKER));

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
            if (!lookupHashTable(offsets, *bufptr)) { //
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
            ip = get_closure_info((StgClosure*) bufptr, INFO_PTR_TO_STRUCT(ip),
                    &clsize, &ptrs, &nonptrs, &vhs);

            // IF_PAR_DEBUG(pack,debugBelch("size (%ld + %d + %d +%d, = %d)",
            //              HEADERSIZE, vhs, ptrs, nonptrs, clsize));

            // This is rather a test for get_closure_info...but used here
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
                    (StgWord) ( bufptr - (packBuffer->buffer)),
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

    } while (openptrs != 0 && packsize < packBuffer->size);

    IF_PAR_DEBUG(pack,
            debugBelch(" traversed %" FMT_Word " words, %"
                FMT_Word " open pointers ", packsize, openptrs));

    if (openptrs != 0) {
        barf("%d open pointers at end of packet ",
                openptrs);
    }

    IF_DEBUG(sanity, ASSERT(*(bufptr++) == END_OF_BUFFER_MARKER && packsize++));

    if (packsize != packBuffer->size) {
        barf("surplus data (%" FMT_Word " words) at end of packet ",
                packBuffer->size - packsize);
    }

    freeHashTable(offsets, NULL);
    IF_PAR_DEBUG(pack, debugBelch("packet OK\n"));

}

/* END OF DEBUG */
#endif

