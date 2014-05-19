
#include <Rts.h>
#include "Utils.h"

void *
pmMallocBytes (int n, char *msg)
{
    char *space;
    size_t n2;

    n2 = (size_t) n;
    if ((space = (char *) malloc(n2)) == NULL) {
      /* don't fflush(stdout); WORKAROUND bug in Linux glibc */
      MallocFailHook((W_) n, msg); /*msg*/
      stg_exit(EXIT_INTERNAL_ERROR);
    }
    return space;
}

void *
pmReallocBytes (void *p, int n, char *msg)
{
    char *space;
    size_t n2;

    n2 = (size_t) n;
    if ((space = (char *) realloc(p, (size_t) n2)) == NULL) {
      /* don't fflush(stdout); WORKAROUND bug in Linux glibc */
      MallocFailHook((W_) n, msg); /*msg*/
      stg_exit(EXIT_INTERNAL_ERROR);
    }
    return space;
}

void *
pmCallocBytes (int n, int m, char *msg)
{
    char *space;

    if ((space = (char *) calloc((size_t) n, (size_t) m)) == NULL) {
      /* don't fflush(stdout); WORKAROUND bug in Linux glibc */
      MallocFailHook((W_) n*m, msg); /*msg*/
      stg_exit(EXIT_INTERNAL_ERROR);
    }
    return space;
}

/* To simplify changing the underlying allocator used
 * by pmMallocBytes(), provide pmFree() as well.
 */
void
pmFree(void* p)
{
  free(p);
}
