#ifndef VAL_LIMIT_H
#define VAL_LIMIT_H

#include "hash.h"

extern "C" {
  my_bool val_limit_init(UDF_INIT *initid, UDF_ARGS *args, char *message);

  longlong val_limit(UDF_INIT *initid, UDF_ARGS *args,
                     char *is_null,
                     char *error);
  void val_limit_deinit(UDF_INIT *initid);
}

typedef struct val_limit
{
  HASH seen_hash;
  longlong limit;
} val_limit_t;

#endif
