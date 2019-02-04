
#include <cassert>
#include <cstdlib>
#include <string.h>     /* for memcpy() etc.        */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <limits.h>


#if defined(__cplusplus)
extern "C" {
#endif


/* Try this if you you get an error from one of the typedefs below */
#ifdef BRG_STD_TYPES
#  define BRG_UI8
     typedef u_int8_t uint_8t;
#  define BRG_UI16
     typedef u_int16_t uint_16t;
#  define BRG_UI32
#    define li_32(h) 0x##h##u
     typedef u_int32_t uint_32t;
#  define BRG_UI64
#    define li_64(h) 0x##h##u
     typedef u_int64_t uint_64t;
#endif /* BRG_C99_TYPES */

#ifndef BRG_UI8
#  define BRG_UI8
#  if UCHAR_MAX == 255u
     typedef unsigned char uint_8t;
#  else
#    error Please define uint_8t as an 8-bit unsigned integer type in brg_types.h
#  endif
#endif

#ifndef BRG_UI16
#  define BRG_UI16
#  if USHRT_MAX == 65535u
     typedef unsigned short uint_16t;
#  else
#    error Please define uint_16t as a 16-bit unsigned short type in brg_types.h
#  endif
#endif

#ifndef BRG_UI32
#  define BRG_UI32
#  if UINT_MAX == 4294967295u
#    define li_32(h) 0x##h##u
     typedef unsigned int uint_32t;
#  elif ULONG_MAX == 4294967295u
#    define li_32(h) 0x##h##ul
     typedef unsigned long uint_32t;
#  elif defined( _CRAY )
#    error This code needs 32-bit data types, which Cray machines do not provide
#  else
#    error Please define uint_32t as a 32-bit unsigned integer type in brg_types.h
#  endif
#endif

#ifndef BRG_UI64
#  if defined( __BORLANDC__ ) && !defined( __MSDOS__ )
#    define BRG_UI64
#    define li_64(h) 0x##h##ull
     typedef unsigned __int64 uint_64t;
#  elif defined( _MSC_VER ) && ( _MSC_VER < 1300 )    /* 1300 == VC++ 7.0 */
#    define BRG_UI64
#    define li_64(h) 0x##h##ui64
     typedef unsigned __int64 uint_64t;
#  elif defined( __sun ) && defined(ULONG_MAX) && ULONG_MAX == 0xfffffffful
#    define BRG_UI64
#    define li_64(h) 0x##h##ull
     typedef unsigned long long uint_64t;
#  elif defined( UINT_MAX ) && UINT_MAX > 4294967295u
#    if UINT_MAX == 18446744073709551615u
#      define BRG_UI64
#      define li_64(h) 0x##h##u
       typedef unsigned int uint_64t;
#    endif
#  elif defined( ULONG_MAX ) && ULONG_MAX > 4294967295u
#    if ULONG_MAX == 18446744073709551615ul
#      define BRG_UI64
#      define li_64(h) 0x##h##ul
       typedef unsigned long uint_64t;
#    endif
#  elif defined( ULLONG_MAX ) && ULLONG_MAX > 4294967295u
#    if ULLONG_MAX == 18446744073709551615ull
#      define BRG_UI64
#      define li_64(h) 0x##h##ull
       typedef unsigned long long uint_64t;
#    endif
#  elif defined( ULONG_LONG_MAX ) && ULONG_LONG_MAX > 4294967295u
#    if ULONG_LONG_MAX == 18446744073709551615ull
#      define BRG_UI64
#      define li_64(h) 0x##h##ull
       typedef unsigned long long uint_64t;
#    endif
#  endif
#endif

#if defined( NEED_UINT_64T ) && !defined( BRG_UI64 )
#  error Please define uint_64t as an unsigned 64 bit type in brg_types.h
#endif

#ifndef RETURN_VALUES
#  define RETURN_VALUES
#  if defined( DLL_EXPORT )
#    if defined( _MSC_VER ) || defined ( __INTEL_COMPILER )
#      define VOID_RETURN    __declspec( dllexport ) void __stdcall
#      define INT_RETURN     __declspec( dllexport ) int  __stdcall
#    elif defined( __GNUC__ )
#      define VOID_RETURN    __declspec( __dllexport__ ) void
#      define INT_RETURN     __declspec( __dllexport__ ) int
#    else
#      error Use of the DLL is only available on the Microsoft, Intel and GCC compilers
#    endif
#  elif defined( DLL_IMPORT )
#    if defined( _MSC_VER ) || defined ( __INTEL_COMPILER )
#      define VOID_RETURN    __declspec( dllimport ) void __stdcall
#      define INT_RETURN     __declspec( dllimport ) int  __stdcall
#    elif defined( __GNUC__ )
#      define VOID_RETURN    __declspec( __dllimport__ ) void
#      define INT_RETURN     __declspec( __dllimport__ ) int
#    else
#      error Use of the DLL is only available on the Microsoft, Intel and GCC compilers
#    endif
#  elif defined( __WATCOMC__ )
#    define VOID_RETURN  void __cdecl
#    define INT_RETURN   int  __cdecl
#  else
#    define VOID_RETURN  void
#    define INT_RETURN   int
#  endif
#endif

/*  These defines are used to declare buffers in a way that allows
    faster operations on longer variables to be used.  In all these
    defines 'size' must be a power of 2 and >= 8

    dec_unit_type(size,x)       declares a variable 'x' of length
                                'size' bits

    dec_bufr_type(size,bsize,x) declares a buffer 'x' of length 'bsize'
                                bytes defined as an array of variables
                                each of 'size' bits (bsize must be a
                                multiple of size / 8)

    ptr_cast(x,size)            casts a pointer to a pointer to a
                                varaiable of length 'size' bits
*/

#define ui_type(size)               uint_##size##t
#define dec_unit_type(size,x)       typedef ui_type(size) x
#define dec_bufr_type(size,bsize,x) typedef ui_type(size) x[bsize / (size >> 3)]
#define ptr_cast(x,size)            ((ui_type(size)*)(x))

#if defined(__cplusplus)
}
#endif


// -----------------------------------------------------------------------------------------


#define SHA1_BLOCK_SIZE  64
#define SHA1_DIGEST_SIZE 20

#if defined(__cplusplus)
extern "C"
{
#endif

struct sha1_ctx_s
{   uint_32t count[2];
    uint_32t hash[5];
    uint_32t wbuf[16];
};

typedef struct sha1_ctx_s sha1_ctx;

/* Note that these prototypes are the same for both bit and */
/* byte oriented implementations. However the length fields */
/* are in bytes or bits as appropriate for the version used */
/* and bit sequences are input as arrays of bytes in which  */
/* bit sequences run from the most to the least significant */
/* end of each byte                                         */

VOID_RETURN sha1_compile(sha1_ctx ctx[1]);

VOID_RETURN sha1_begin(sha1_ctx ctx[1]);
VOID_RETURN sha1_hash(const unsigned char data[], unsigned len, sha1_ctx ctx[1]);
VOID_RETURN sha1_end(unsigned char hval[], sha1_ctx ctx[1]);
VOID_RETURN sha1(unsigned char hval[], const unsigned char data[], unsigned long len);

#if defined(__cplusplus)
}
#endif



// -----------------------------------------------------------------------------------------


#if defined(__cplusplus)
extern "C"
{
#endif

#if defined( _MSC_VER ) && ( _MSC_VER > 800 )
#pragma intrinsic(memcpy)
#endif

#if 0 && defined(_MSC_VER)
#define rotl32  _lrotl
#define rotr32  _lrotr
#else
#define rotl32(x,n)   (((x) << n) | ((x) >> (32 - n)))
#define rotr32(x,n)   (((x) >> n) | ((x) << (32 - n)))
#endif

#if !defined(bswap_32)
#define bswap_32(x) ((rotr32((x), 24) & 0x00ff00ff) | (rotr32((x), 8) & 0xff00ff00))
#endif

#if (PLATFORM_BYTE_ORDER == IS_LITTLE_ENDIAN)
#define SWAP_BYTES
#else
#undef  SWAP_BYTES
#endif

#if defined(SWAP_BYTES)
#define bsw_32(p,n) \
    { int _i = (n); while(_i--) ((uint_32t*)p)[_i] = bswap_32(((uint_32t*)p)[_i]); }
#else
#define bsw_32(p,n)
#endif

#define SHA1_MASK   (SHA1_BLOCK_SIZE - 1)

#if 0

#define ch(x,y,z)       (((x) & (y)) ^ (~(x) & (z)))
#define parity(x,y,z)   ((x) ^ (y) ^ (z))
#define maj(x,y,z)      (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))

#else   /* Discovered by Rich Schroeppel and Colin Plumb   */

#define ch(x,y,z)       ((z) ^ ((x) & ((y) ^ (z))))
#define parity(x,y,z)   ((x) ^ (y) ^ (z))
#define maj(x,y,z)      (((x) & (y)) | ((z) & ((x) ^ (y))))

#endif

/* Compile 64 bytes of hash data into SHA1 context. Note    */
/* that this routine assumes that the byte order in the     */
/* ctx->wbuf[] at this point is in such an order that low   */
/* address bytes in the ORIGINAL byte stream will go in     */
/* this buffer to the high end of 32-bit words on BOTH big  */
/* and little endian systems                                */

#ifdef ARRAY
#define q(v,n)  v[n]
#else
#define q(v,n)  v##n
#endif

#define one_cycle(v,a,b,c,d,e,f,k,h)            \
    q(v,e) += rotr32(q(v,a),27) +               \
              f(q(v,b),q(v,c),q(v,d)) + k + h;  \
    q(v,b)  = rotr32(q(v,b), 2)

#define five_cycle(v,f,k,i)                 \
    one_cycle(v, 0,1,2,3,4, f,k,hf(i  ));   \
    one_cycle(v, 4,0,1,2,3, f,k,hf(i+1));   \
    one_cycle(v, 3,4,0,1,2, f,k,hf(i+2));   \
    one_cycle(v, 2,3,4,0,1, f,k,hf(i+3));   \
    one_cycle(v, 1,2,3,4,0, f,k,hf(i+4))

VOID_RETURN sha1_compile(sha1_ctx ctx[1])
{   uint_32t    *w = ctx->wbuf;

#ifdef ARRAY
    uint_32t    v[5];
    memcpy(v, ctx->hash, 5 * sizeof(uint_32t));
#else
    uint_32t    v0, v1, v2, v3, v4;
    v0 = ctx->hash[0]; v1 = ctx->hash[1];
    v2 = ctx->hash[2]; v3 = ctx->hash[3];
    v4 = ctx->hash[4];
#endif

#define hf(i)   w[i]

    five_cycle(v, ch, 0x5a827999,  0);
    five_cycle(v, ch, 0x5a827999,  5);
    five_cycle(v, ch, 0x5a827999, 10);
    one_cycle(v,0,1,2,3,4, ch, 0x5a827999, hf(15)); \

#undef  hf
#define hf(i) (w[(i) & 15] = rotl32(                    \
                 w[((i) + 13) & 15] ^ w[((i) + 8) & 15] \
               ^ w[((i) +  2) & 15] ^ w[(i) & 15], 1), w[(i) & 15])

    one_cycle(v,4,0,1,2,3, ch, 0x5a827999, hf(16));
    one_cycle(v,3,4,0,1,2, ch, 0x5a827999, hf(17));
    one_cycle(v,2,3,4,0,1, ch, 0x5a827999, hf(18));
    one_cycle(v,1,2,3,4,0, ch, 0x5a827999, hf(19));

    five_cycle(v, parity, 0x6ed9eba1,  20);
    five_cycle(v, parity, 0x6ed9eba1,  25);
    five_cycle(v, parity, 0x6ed9eba1,  30);
    five_cycle(v, parity, 0x6ed9eba1,  35);

    five_cycle(v, maj, 0x8f1bbcdc,  40);
    five_cycle(v, maj, 0x8f1bbcdc,  45);
    five_cycle(v, maj, 0x8f1bbcdc,  50);
    five_cycle(v, maj, 0x8f1bbcdc,  55);

    five_cycle(v, parity, 0xca62c1d6,  60);
    five_cycle(v, parity, 0xca62c1d6,  65);
    five_cycle(v, parity, 0xca62c1d6,  70);
    five_cycle(v, parity, 0xca62c1d6,  75);

#ifdef ARRAY
    ctx->hash[0] += v[0]; ctx->hash[1] += v[1];
    ctx->hash[2] += v[2]; ctx->hash[3] += v[3];
    ctx->hash[4] += v[4];
#else
    ctx->hash[0] += v0; ctx->hash[1] += v1;
    ctx->hash[2] += v2; ctx->hash[3] += v3;
    ctx->hash[4] += v4;
#endif
}

VOID_RETURN sha1_begin(sha1_ctx ctx[1])
{
    ctx->count[0] = 0;
	ctx->count[1] = 0;
    ctx->hash[0] = 0x67452301;
    ctx->hash[1] = 0xefcdab89;
    ctx->hash[2] = 0x98badcfe;
    ctx->hash[3] = 0x10325476;
    ctx->hash[4] = 0xc3d2e1f0;
}

/* SHA1 hash data in an array of bytes into hash buffer and */
/* call the hash_compile function as required.              */

VOID_RETURN sha1_hash(const unsigned char data[], unsigned len, sha1_ctx ctx[1])
{   uint_32t pos = (uint_32t)(ctx->count[0] & SHA1_MASK),
            space = SHA1_BLOCK_SIZE - pos;
    const unsigned char *sp = data;

    ctx->count[0] += len;
    if(ctx->count[0] < len)
        ++(ctx->count[1]);

    while(len >= space)     /* transfer whole blocks if possible  */
    {
		unsigned char* tmp = (unsigned char*)(ctx->wbuf);
        memcpy(tmp + pos, sp, space);
        sp += space; len -= space; space = SHA1_BLOCK_SIZE; pos = 0;
        bsw_32(ctx->wbuf, SHA1_BLOCK_SIZE >> 2);
        sha1_compile(ctx);
    }

	unsigned char* tmp = (unsigned char*)(ctx->wbuf);
    memcpy(tmp + pos, sp, len);
}

/* SHA1 final padding and digest calculation  */

VOID_RETURN sha1_end(unsigned char hval[], sha1_ctx ctx[1])
{   uint_32t    i = (uint_32t)(ctx->count[0] & SHA1_MASK);

    /* put bytes in the buffer in an order in which references to   */
    /* 32-bit words will put bytes with lower addresses into the    */
    /* top of 32 bit words on BOTH big and little endian machines   */
    bsw_32(ctx->wbuf, (i + 3) >> 2);

    /* we now need to mask valid bytes and add the padding which is */
    /* a single 1 bit and as many zero bits as necessary. Note that */
    /* we can always add the first padding byte here because the    */
    /* buffer always has at least one empty slot                    */
    ctx->wbuf[i >> 2] &= 0xffffff80 << 8 * (int)(~i & 3);
    ctx->wbuf[i >> 2] |= 0x00000080 << 8 * (int)(~i & 3);

    /* we need 9 or more empty positions, one for the padding byte  */
    /* (above) and eight for the length count. If there is not      */
    /* enough space, pad and empty the buffer                       */
    if(i > SHA1_BLOCK_SIZE - 9)
    {
        if(i < 60) ctx->wbuf[15] = 0;
        sha1_compile(ctx);
        i = 0;
    }
    else    /* compute a word index for the empty buffer positions  */
        i = (i >> 2) + 1;

    while(i < 14) /* and zero pad all but last two positions        */
        ctx->wbuf[i++] = 0;

    /* the following 32-bit length fields are assembled in the      */
    /* wrong byte order on little endian machines but this is       */
    /* corrected later since they are only ever used as 32-bit      */
    /* word values.                                                 */
    ctx->wbuf[14] = (ctx->count[1] << 3) | (ctx->count[0] >> 29);
    ctx->wbuf[15] = ctx->count[0] << 3;
    sha1_compile(ctx);

    /* extract the hash value as bytes in case the hash buffer is   */
    /* misaligned for 32-bit words                                  */
    for(i = 0; i < SHA1_DIGEST_SIZE; ++i)
        hval[i] = (unsigned char)(ctx->hash[i >> 2] >> (8 * (int)(~i & 3)));
}

VOID_RETURN sha1(unsigned char hval[], const unsigned char data[], unsigned long len)
{   sha1_ctx    cx[1];

    sha1_begin(cx); sha1_hash(data, len, cx); sha1_end(hval, cx);
}

#if defined(__cplusplus)
}
#endif


// -----------------------------------------------------------------------------------------

void kill_cycles(long iterations)
{
    assert(iterations >= 0);

    unsigned char data[] = {'H', 'e', 'l', 'l', 'o', ' ',
                            'W', 'o', 'r', 'l', 'd' };

    unsigned char hash[SHA1_DIGEST_SIZE] = {0};
    {
        sha1_ctx ctx[1];
        sha1_begin(ctx);
        for (long i = 0; i < iterations; i++) {
            sha1_hash(data, sizeof(data) - 1, ctx);
        }
        sha1_end(hash, ctx);
    }

    // do some stupid check so the compiler does not remove this due to
    // optimization
    {
        bool zero = true;

        for (int i = 0; i < int(sizeof(hash)); i++) {
            if (hash[i] != 0) {
                zero = false;
            }
        }

        if (zero) {
            abort();
        }
    }
}
