#include <stdio.h>

#include "Rts.h"

int pack(StgClosure *node) {
    StgInfoTable *info;
    if (node->header.info == NULL) {
        printf("header is null\n");
        return 0;
    }
    node = UNTAG_CLOSURE(node);
    info = get_itbl(node);
    printf("%x\n", info);
    printf("%x\n", node->header.info);
    switch (info->type) {
        case CONSTR:
        case CONSTR_1_0:
        case CONSTR_0_1:
        case CONSTR_2_0:
        case CONSTR_1_1:
        case CONSTR_0_2:
        case CONSTR_STATIC:
        case CONSTR_NOCAF_STATIC:
            printf("constructor\n");
            return 1;
        default:
            printf("something else\n");
            return 0;
    }
}

