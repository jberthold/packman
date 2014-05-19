#ifndef PACKMAN_UTILS_H
#define PACKMAN_UTILS_H

void *pmMallocBytes(int n, char *msg)
    GNUC3_ATTRIBUTE(__malloc__);

void *pmReallocBytes(void *p, int n, char *msg);

void *pmCallocBytes(int n, int m, char *msg)
     GNUC3_ATTRIBUTE(__malloc__);

void pmFree(void* p);

#endif /* PACKMAN_UTILS_H */
