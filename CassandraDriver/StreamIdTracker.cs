using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace CassandraDriver;

// Note for other contributers. If you touch this, make sure there is no race conditions - Instellate
/// <summary>
/// A class that tracks the stream IDs. Used by the client to generate conflict free IDs
/// </summary>
/// <remarks>
/// This allocates all numbers from 0 to 32767 as int32.
/// This will take 128kb in memory and there will exist one instance for each client.
/// If you have a slow system, startup times might be slower as it needs filling up the array with all the numbers.
/// The reason why this is done is to track what numbers are in use and not in use and also allow releasing of numbers.
/// This is why this system is conflict free.
///
/// The tracker will also throw an exception if somehow all the 32768 possible stream IDs are used simultaneous.
/// If you have this issue you should look into scaling up your nodes or load balance properly.
/// </remarks>
public class StreamIdTracker
{
    private const int BufferSize = 32768;
    private const int FragmentAmount = 8192;

    [InlineArray(BufferSize)]
    private struct Buffer
    {
        private int _element;
    }

    private Buffer _buffer = new();

    /// <summary>
    /// Constructs the tracker.
    /// </summary>
    /// <remarks>This tracker takes some time to construct as it fills up an array with 32768 numbers</remarks>
    public StreamIdTracker()
    {
        Span<int> span = this._buffer;

        for (int i = 0; i < BufferSize; i++)
        {
            span[i] = i;
        }
    }

    /// <summary>
    /// Acquires a stream ID
    /// </summary>
    /// <returns>Returns a found stream ID</returns>
    /// <exception cref="CassandraException">There is no more stream IDs to acquire. See <see cref="StreamIdTracker"/> remark for more information</exception>
    public short AcquireStreamId()
    {
        Span<int> span = this._buffer;
        Span<int> fragment = span;

        for (int i = 0; i < FragmentAmount; i++)
        {
            int maxNum;
            int temp;
            if ((temp = Volatile.Read(ref fragment[0])) >= 0)
            {
                maxNum = temp;
            }
            else if ((temp = Volatile.Read(ref fragment[1])) >= 0)
            {
                maxNum = temp;
            }
            else if ((temp = Volatile.Read(ref fragment[2])) >= 0)
            {
                maxNum = temp;
            }
            else if ((temp = Volatile.Read(ref fragment[3])) >= 0)
            {
                maxNum = temp;
            }
            else
            {
                fragment = fragment[4..];
                continue;
            }

            int value = maxNum == 0 ? -32767 : -maxNum;
            if (Interlocked.CompareExchange(ref span[maxNum], value, maxNum) ==
                maxNum)
            {
                return (short)maxNum;
            }
        }

        throw new CassandraException("All possible stream IDs are already occupied");
    }

    /// <summary>
    /// Releases a stream ID
    /// </summary>
    /// <param name="id">The stream ID to release</param>
    /// <returns></returns>
    public void ReleaseStreamId(short id)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(id);

        Span<int> span = this._buffer;

        Volatile.Write(ref span[id], id);
    }
}
