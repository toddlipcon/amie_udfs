/**
 * Aggregate functions:
 *  BITSET_AGGREGATE(int column, int max_width)
 *     returns a bitstring with those integer bits set
 *
 * Non-aggregate functions:
 *  BITSET_OR(bitset a, bitset b)
 *     returns the bitwise or of the two arguments
 *  BITSET_AND(bitset a, bitset b)
 *     returns the bitwise and of the two arguments
 *  BITSET_CREATE(ints...)
 *     returns a new bitset with the given integers set
 */

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

/* bitset is always a multiple of this number of bytes */
#define CHUNK_SIZE 8
#define MAX_SIZE 128

typedef struct bitset
{
  size_t len;
  size_t max_len;
  unsigned char *data;
} bitset_t;


extern "C" {
  my_bool bitset_aggregate_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void bitset_aggregate_deinit(UDF_INIT *initid);
  void bitset_aggregate_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *message);
  void bitset_aggregate_add(UDF_INIT *initid, UDF_ARGS *args,
                            char *is_null, char *error);
  char *bitset_aggregate(UDF_INIT *initid, UDF_ARGS *args,
                         char *result, unsigned long *length,
                         char *is_null, char *message);
  void bitset_aggregate_clear(UDF_INIT *initid, char *is_null, char *message);


  my_bool bitset_or_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  void bitset_or_deinit(UDF_INIT *initid);
  char *bitset_or(UDF_INIT *initid, UDF_ARGS *args,
                  char *result, unsigned long *length,
                  char *is_null, char *message);

}


static bitset_t *bitset_new(size_t initial_len, size_t max_len)
{
  bitset_t *data = (bitset_t *)malloc(sizeof(bitset_t));
  if (!data)
    return NULL;

  data->len = initial_len;
  data->max_len = max_len;
  data->data = (unsigned char *)calloc(initial_len, 1);
  if (!data->data)
  {
    free(data);
    return NULL;
  }

  return data;
}

static void bitset_free(bitset_t *bs)
{
  fprintf(stderr, "in bitset_free");
  if (bs->data)
  {
    fprintf(stderr, "freeing bs data\n");
    free(bs->data);
    bs->data = NULL;
  }
  bs->len = 0;
}

static void bitset_clear(bitset_t *bs)
{
  fprintf(stderr, "bitset_clear");
  bzero(bs->data, bs->len);
}

static my_bool bitset_ensure_len(bitset_t *bs, size_t len)
{
  if (len > bs->max_len)
  {
    fprintf(stderr, "byte is too big!\n");
    return false; // too high
  }

  // Check for resize
  if (len > bs->len)
  {
    fprintf(stderr, "Resizing - cur len is %d and need len %d\n", (int)bs->len, (int)len);
    size_t new_size;
    size_t chunk_mod = len % CHUNK_SIZE;
    if (chunk_mod == 0)
      new_size = len;
    else
      new_size = len + (CHUNK_SIZE - chunk_mod);

    bs->data = (unsigned char *)realloc(bs->data, new_size);
    bs->len = new_size;
    if (!bs->data)
    {
      // TODO warning/error
      return false;
    }
  }

  return true;
}

static void bitset_set(bitset_t *bs, size_t bit) {
  if (bs->data == NULL)
    return; // a previous realloc failed

  fprintf(stderr, "Bit: %d\n", (int)bit);

  size_t byte = bit/8;
  size_t bit_in_byte = bit % 8;

  fprintf(stderr, "Byte: %d\tbib: %d\n", (int)byte, (int)bit_in_byte);

  if (!bitset_ensure_len(bs, byte + 1))
    return;
  
  bs->data[byte] |= 1 << bit_in_byte;
}

static void bitset_merge_data(bitset_t *bs, char *data, size_t datalen) {
  if (!bitset_ensure_len(bs, datalen))
    return;

  for (uint i = 0; i < datalen; i++)
  {
    bs->data[i] |= data[i];
  }
}

/************************************************************/

my_bool bitset_aggregate_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  longlong maxlen;

  initid->maybe_null = false;
  if (args->arg_count != 2)
  {
    strmov(message, "usage: BITSET_AGGREGATE(column, max_width)");
    goto err;
  }

  if (args->arg_type[0] != INT_RESULT)
  {
    strmov(message, "first argument to BITSET_AGGREGATE should be an INT");
    goto err;
  }

  if (args->arg_type[1] != INT_RESULT ||
      args->args[1] == 0)
  {
    strmov(message, "second argument to BITSET_AGGREGATE should be a constant INT");
    goto err;
  }

  maxlen = *((longlong*) args->args[1]);
  if (maxlen > MAX_SIZE || maxlen <= 0)
  {
    strmov(message, "max len for BITSET_AGGREGATE must be between 0 and some reasonable max");
    goto err;
  }

  if (!(initid->ptr = (char *)bitset_new((size_t)maxlen, (size_t)maxlen))) {
    strmov(message, "Couldn't create empty bitset");
    goto err;
  }


  initid->max_length = maxlen;

  return 0;

  err:
  if (initid->ptr)
    bitset_free((bitset_t *)initid->ptr);
  return 1;
}

void bitset_aggregate_deinit(UDF_INIT *initid)
{
  if (initid->ptr)
    bitset_free((bitset_t *)initid->ptr);
}

void bitset_aggregate_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *message)
{
  bitset_t *bs = (bitset_t *)initid->ptr;
  if (!bs)
    return;

  bitset_clear(bs);
  bitset_aggregate_add(initid, args, is_null, message);
}

void bitset_aggregate_clear(UDF_INIT *initid, char *is_null, char *message)
{
  bitset_t *bs = (bitset_t *)initid->ptr;
  if (!bs)
    return;

  bitset_clear(bs);  
}

void bitset_aggregate_add(UDF_INIT *initid, UDF_ARGS *args,
                          char *is_null, char *error)
{
  bitset_t *bs = (bitset_t *)initid->ptr;
  if (args->args[0] == NULL) {
    return;
  }

  longlong bit = *((longlong*) args->args[0]);

  if (bit < 0) {
    // warning? how to throw warning from udf?
    return;
  }

  bitset_set(bs, (size_t)bit);
}

char *bitset_aggregate(UDF_INIT *initid, UDF_ARGS *args,
                       char *result, unsigned long *length,
                       char *is_null, char *message)
{
  bitset_t *bs = (bitset_t *)initid->ptr;
  if (!bs)
  {
    *is_null = 1;
    return NULL;
  }

  *is_null = 0;
  initid->max_length = bs->len;
  *length = bs->len;
  return (char *)bs->data;
}

/************************************************************/

my_bool bitset_or_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  uint i, max_length=0;

  if (args->arg_count < 2)
  {
    strmov(message, "BITSET_OR requires at least two arguments");
    return 1;
  }

  for (i = 0; i < args->arg_count; i++)
  {
    if (args->arg_type[1] != STRING_RESULT)
    {
      strmov(message, "BITSET_OR arguments must be BINARY");
      return 1;
    }

    if (args->lengths[i] > max_length)
      max_length = args->lengths[i];
  }
  initid->max_length = max_length;
  initid->maybe_null = 1; /* if all args are null */
  initid->ptr = NULL;
  return 0;
}

void bitset_or_deinit(UDF_INIT *initid)
{
  bitset_t *bs = (bitset_t *)initid->ptr;
  if (bs)
  {
    bitset_free(bs);
    initid->ptr = NULL;
  }
}

char *bitset_or(UDF_INIT *initid, UDF_ARGS *args,
                char *result, unsigned long *length,
                char *is_null, char *message)
{
  /* first determine length and whether it should be null */

  uint max_len = 0;
  *is_null = 1;
  for (uint i = 0; i < args->arg_count; i++)
  {
    if (args->args[i] == NULL)
      continue;
    *is_null = 0;
    if (args->lengths[i] > max_len)
      max_len = args->lengths[i];
  }

  if (*is_null)
  {
    return NULL;
  }

  /* now allocate the bitset */
  bitset_t *bs = bitset_new(max_len, max_len);
  for (uint i = 0; i < args->arg_count; i++)
  {
    bitset_merge_data(bs, args->args[i], args->lengths[i]);
  }

  *length = max_len;
  if (max_len < 255)
  {
    /* result buffer mysql provides is 255 long */
    memcpy(result, bs->data, max_len);
    bitset_free(bs);
    return result;

  } else {
    initid->ptr = (char  *)bs;

    return (char *)bs->data;
  }

}
