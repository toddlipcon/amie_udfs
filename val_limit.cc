#ifdef STANDARD
  #include <stdio.h>
  #include <string.h>
  #ifdef __WIN__
    typedef unsigned __int64 ulonglong;	
    typedef __int64 longlong;
  #else
    typedef unsigned long long ulonglong;
    typedef long long longlong;
  #endif /*__WIN__*/
#else
  #include <my_global.h>
  #include <my_sys.h>
#endif

#include <mysql.h>
#include <m_ctype.h>
#include <m_string.h>
#include "hash.h"
#include "val_limit.h"

/* Implementation for the HASH structure */
typedef struct seen_rec_t {
  uint count;
  uint keylen;
  byte keydata[0]; // key data will get put here
};


static byte *seenhash_get_key(const byte *data,
                       uint *length,
                       my_bool not_used __attribute__((unused)))
{
  const seen_rec_t *rec = (const seen_rec_t *)data;
  *length = rec->keylen;
  return (byte *)rec->keydata;
}

static void seenhash_free(void *record)
{
  my_free((char *)record,MYF(0));
}



/* Initialize storage */
my_bool val_limit_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->maybe_null = false;

  val_limit_t *data = NULL;

  if (!(data = (val_limit_t *)my_malloc(sizeof(val_limit_t), MYF(MY_FAE))))
  {
    strmov(message, "Couldn't allocate memory");
    goto err;
  }
  initid->ptr = (char *)data;

  hash_clear(&data->seen_hash);
  

  /* check number of arguments */
  if (args->arg_count != 2)
  {
    strmov(message, "VAL_LIMIT() requires two arguments");
    goto err;
  }

  /* deal with first parameter (the column) */
  if (args->arg_type[0] != INT_RESULT ||
      args->args[0] != 0)
  {
    strmov(message, "VAL_LIMIT() requires a non-constant integer as its first argument");
    goto err;
  }

  /* deal with second parameter (the number of unique vals to permit) */
  if (args->arg_type[1] != INT_RESULT ||
      args->args[1] == 0)
  {
    strmov(message, "VAL_LIMIT() requires a constant integer as its second argument");
    goto err;
  }

  data->limit = *((longlong*) args->args[1]);

  /* now set up the hash structure based on the type of the column argument */
  if (hash_init(&data->seen_hash, &my_charset_bin, 32,
                offsetof(seen_rec_t, keydata), 0, seenhash_get_key,
                seenhash_free, 0))
  {
    strmov(message, "Could not allocate hash");
    goto err;
  }

  return 0;

err:
  if (data != NULL)
    free(data);
  return 1;
}

static my_bool add_item(HASH *seen_hash, byte *key, uint keylen, uint *count) {
  seen_rec_t *found_rec =
    (seen_rec_t *)hash_search(seen_hash, key, keylen);

  if (found_rec == NULL) {
    seen_rec_t *rec = (seen_rec_t *)my_malloc(
      sizeof(seen_rec_t) + keylen, MYF(MY_FAE));

    rec->count = 1;
    rec->keylen = keylen;
    memcpy(rec->keydata, key, keylen);

    if (my_hash_insert(seen_hash, (byte *)rec))
    {
      return 1;
    }
    *count = 1;
  } else {
    *count = ++found_rec->count;
  }

  return 0;
}

static my_bool add_int_item(HASH *seen_hash, longlong val, uint *count) {
  byte *searchkey = (byte *)&val;

  return add_item(seen_hash, searchkey, sizeof(longlong), count);
}


longlong val_limit(UDF_INIT *initid, UDF_ARGS *args,
                   char *is_null,
                   char *error)
{
  val_limit_t *data = (val_limit_t *)initid->ptr;

  if (*is_null)
    return 1; /* pass through all nulls */

  longlong val= *((longlong*) args->args[0]);

  uint count = 0;
  my_bool err = add_int_item(&data->seen_hash, val, &count);
  if (err) {
    *error = 1;
    return 0;
  }

  if (count <= data->limit)
    return 1;
  else
    return 0;
}

void val_limit_deinit(UDF_INIT *initid) {
  val_limit_t *data = (val_limit_t *)initid->ptr;

  if (data != NULL)
  {
    if (hash_inited(&data->seen_hash))
      hash_free(&data->seen_hash);
    free(data);
  }
}
