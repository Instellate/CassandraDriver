using System;

namespace CassandraDriver;

public static class CassandraMurmur3Hash
{
    public static long CalculatePrimaryKey(ReadOnlySpan<byte> key)
    {
        return CalculateHash(key, 0).Item1;
    }

    public static unsafe (long, long) CalculateHash(ReadOnlySpan<byte> key, ulong seed)
    {
        Span<long> result = stackalloc long[2];

        fixed (byte* pKey = key)
        fixed (long* pResult = result)
        {
            MurmurHash3_x64_128_cassandra(pKey,
                key.Length,
                unchecked((long)seed),
                pResult);
        }

        return (result[0], result[1]);
    }

    private static long Rotsl64(long x, sbyte r)
    {
        return (long)((ulong)(x << r) | ((ulong)x >> (64 - r)));
    }

    private static long Sfmix(long k)
    {
        k ^= k >>> 33;
        k *= unchecked((long)0xff51afd7ed558ccd);
        k ^= k >>> 33;
        k *= unchecked((long)0xc4ceb9fe1a85ec53L);
        k ^= k >>> 33;

        return k;
    }

    private static unsafe void MurmurHash3_x64_128_cassandra(void* key, int len,
        long seed,
        void* @out)
    {
        byte* data = (byte*)key;
        int nblocks = len / 16;

        long h1 = seed;
        long h2 = seed;

        long c1 = unchecked((long)0x87c37b91114253d5ul);
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        long* blocks = (long*)data;

        for (int i = 0; i < nblocks; i++)
        {
            long t1 = blocks[i * 2 + 0];
            long t2 = blocks[i * 2 + 1];

            t1 *= c1;
            t1 = Rotsl64(t1, 31);
            t1 *= c2;
            h1 ^= t1;

            h1 = Rotsl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            t2 *= c2;
            t2 = Rotsl64(t2, 33);
            t2 *= c1;
            h2 ^= t2;

            h2 = Rotsl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        sbyte* tail = (sbyte*)(data + nblocks * 16);

        long k1 = 0;
        long k2 = 0;

        switch (len & 15)
        {
            case 15:
                k2 ^= ((long)tail[14]) << 48;
                goto case 14;
            case 14:
                k2 ^= ((long)tail[13]) << 40;
                goto case 13;
            case 13:
                k2 ^= ((long)tail[12]) << 32;
                goto case 12;
            case 12:
                k2 ^= ((long)tail[11]) << 24;
                goto case 11;
            case 11:
                k2 ^= ((long)tail[10]) << 16;
                goto case 10;
            case 10:
                k2 ^= ((long)tail[9]) << 8;
                goto case 9;
            case 9:
                k2 ^= ((long)tail[8]) << 0;
                k2 *= c2;
                k2 = Rotsl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
                goto case 8;
            case 8:
                k1 ^= ((long)tail[7]) << 56;
                goto case 7;
            case 7:
                k1 ^= ((long)tail[6]) << 48;
                goto case 6;
            case 6:
                k1 ^= ((long)tail[5]) << 40;
                goto case 5;
            case 5:
                k1 ^= ((long)tail[4]) << 32;
                goto case 4;
            case 4:
                k1 ^= ((long)tail[3]) << 24;
                goto case 3;
            case 3:
                k1 ^= ((long)tail[2]) << 16;
                goto case 2;
            case 2:
                k1 ^= ((long)tail[1]) << 8;
                goto case 1;
            case 1:
                k1 ^= ((long)tail[0]) << 0;
                k1 *= c1;
                k1 = Rotsl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
                break;
        }

        //----------
        // finalization

        h1 ^= len;
        h2 ^= len;

        h1 += h2;
        h2 += h1;

        h1 = Sfmix(h1);
        h2 = Sfmix(h2);

        h1 += h2;
        h2 += h1;

        ((long*)@out)[0] = h1;
        ((long*)@out)[1] = h2;
    }
}
