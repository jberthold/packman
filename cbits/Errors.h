

/*
 * Return codes for the packing routine (rts/parallel/Pack.c)
 * Must be in sync with library code.
 * We need them here for use in Cmm code in PrimOps.cmm
 */
#define P_SUCCESS       0x00 /* used for return value of PackToMemory only */
#define P_BLACKHOLE     0x01 /* possibly also blocking the packing thread */
#define P_NOBUFFER      0x02 /* buffer too small */
#define P_CANNOTPACK    0x03 /* type cannot be packed (MVar, TVar) */
#define P_UNSUPPORTED   0x04 /* type not supported (but could/should be) */
#define P_IMPOSSIBLE    0x05 /* impossible type found (stack frame,msg, etc) */
#define P_GARBLED       0x06 /* invalid data for deserialisation */
#define P_ERRCODEMAX    0x06

// predicate for checks:
#define isPackError(bufptr) (((StgWord) (bufptr)) <= P_ERRCODEMAX)
