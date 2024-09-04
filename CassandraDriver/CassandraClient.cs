using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Request;
using CassandraDriver.Frames.Response;
using CassandraDriver.Results;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

public class CassandraClient : IDisposable
{
    private readonly Socket _socket;
    private readonly int _port;
    private readonly CancellationTokenSource _tokenSource = new();

    private readonly ConcurrentDictionary<short, TaskCompletionSource<StreamData>>
        _streams = new();

    /// <summary>
    /// Tells if the node is dead. Used by cassandra pool to ignore certain clients.
    /// </summary>
    internal bool IsDead = false;

    internal readonly string Host;

    public bool Connected { get; private set; }
    public string? DefaultKeyspace { get; set; }

    public CassandraClient(string host, int port = 9042, string? defaultKeyspace = null)
    {
        this._socket = new Socket(
            AddressFamily.InterNetworkV6,
            SocketType.Stream,
            ProtocolType.Tcp);
        this.Host = host;
        this._port = port;
        this._socket.Blocking = false;
        this._socket.DualMode = true;
        this._socket.NoDelay = true;
        this.DefaultKeyspace = defaultKeyspace;
    }

    public async Task ConnectAsync()
    {
        await this._socket.ConnectAsync(this.Host, this._port);
        CqlStringMap map = new()
        {
            { "CQL_VERSION", "3.0.0" }
        };

        CqlFrame frame = new(0, CqlOpCode.Startup, map.SizeOf());

        ArrayPoolBufferWriter<byte> writer = new(frame.SizeOf() + map.SizeOf());
        frame.Serialize(writer);
        map.Serialize(writer);

        await this._socket.SendAsync(writer.WrittenMemory);

        byte[] ackResponse = new byte[CqlFrame.CqlFrameConstSize];
        int amountReceived = await this._socket.ReceiveAsync(ackResponse);
        if (amountReceived != CqlFrame.CqlFrameConstSize)
        {
            throw new CassandraException("Frame acquired from server is not big enough");
        }

        CqlFrame ackFrame = CqlFrame.Deserialize(ackResponse);
        if (ackFrame.OpCode == CqlOpCode.Error)
        {
            throw await HandleErrorAsync(ackFrame.Length);
        }
        else if (ackFrame.OpCode != CqlOpCode.Ready)
        {
            throw new CassandraException("Got opcode that isn't error or ready.");
        }

        this.Connected = true;
        _ = HandleReadingAsync(this._tokenSource.Token);
        if (this.DefaultKeyspace is not null)
        {
            await QueryAsync("USE " + this.DefaultKeyspace);
        }
    }

    /// <summary>
    /// Queries the database for a connection. (Not optimum)
    /// </summary>
    /// <param name="query">The query string</param>
    /// <param name="objects">Parameter objects</param>
    /// <returns></returns>
    /// <exception cref="CassandraException"></exception>
    public async Task<Query> QueryAsync(string query, params object[] objects)
    {
        CqlQuery cqlQuery = new(query, objects, CqlConsistency.One);
        short stream = (short)new Random().Next(0, short.MaxValue);
        CqlFrame frame = new(
            stream,
            CqlOpCode.Query,
            cqlQuery.SizeOf()
        );

        ArrayPoolBufferWriter<byte> writer = new(frame.SizeOf() + cqlQuery.SizeOf());
        frame.Serialize(writer);
        cqlQuery.Serialize(writer);


        TaskCompletionSource<StreamData> completionSource = new();
        this._streams.TryAdd(stream, completionSource);
        await this._socket.SendAsync(writer.WrittenMemory);
        StreamData data = await completionSource.Task;

        if (data.Frame.OpCode != CqlOpCode.Result)
        {
            throw new CassandraException("Didn't get result opcode");
        }

        return Query.Deserialize(data.Body.Span, data.Warnings);
    }

    public async Task<Prepared> PrepareAsync(string query)
    {
        CqlLongString queryStr = new(query);
        short stream = (short)new Random().Next(0, short.MaxValue);
        CqlFrame frame = new(stream, CqlOpCode.Prepare, queryStr.SizeOf());

        ArrayPoolBufferWriter<byte> writer = new();
        frame.Serialize(writer);
        queryStr.Serialize(writer);

        TaskCompletionSource<StreamData> completionSource = new();
        this._streams.TryAdd(stream, completionSource);
        await this._socket.SendAsync(writer.WrittenMemory);
        StreamData data = await completionSource.Task;

        QueryKind kind = (QueryKind)BinaryPrimitives.ReadInt32BigEndian(data.Body.Span);
        if (kind != QueryKind.Prepared)
        {
            throw new CassandraException(
                $"Got kind {kind}. Expected kind Prepared"
            );
        }

        Prepared StartDeserialize()
        {
            ReadOnlySpan<byte> bytes = data.Body.Span;
            bytes = bytes[sizeof(QueryKind)..];

            return Prepared.Deserialize(ref bytes);
        }

        return StartDeserialize();
    }

    public async Task<Query> ExecuteAsync(byte[] id, params object[] param)
    {
        CqlExecute cqlExecute = new(id, param, CqlConsistency.One);
        short stream = (short)new Random().Next(0, short.MaxValue);
        CqlFrame frame = new(
            stream,
            CqlOpCode.Execute,
            cqlExecute.SizeOf()
        );

        ArrayPoolBufferWriter<byte> writer = new(frame.SizeOf() + cqlExecute.SizeOf());
        frame.Serialize(writer);
        cqlExecute.Serialize(writer);

        TaskCompletionSource<StreamData> completionSource = new();
        this._streams.TryAdd(stream, completionSource);
        await this._socket.SendAsync(writer.WrittenMemory);
        StreamData data = await completionSource.Task;

        if (data.Frame.OpCode != CqlOpCode.Result)
        {
            throw new CassandraException("Didn't get result opcode");
        }

        return Query.Deserialize(data.Body.Span, data.Warnings);
    }

    public async Task DisconnectAsync()
    {
        await this._tokenSource.CancelAsync();
        await this._socket.DisconnectAsync(true);
        this.Connected = false;
        this._tokenSource.TryReset();
    }

    /// <summary>
    /// Handles errors on the server, always throws
    /// </summary>
    /// <param name="length">The expected body length</param>
    /// <return cref="CassandraException">Thrown error</return>
    private async ValueTask<CassandraException> HandleErrorAsync(int length)
    {
        byte[] errorResp = new byte[length];
        await this._socket.ReceiveAsync(errorResp);
        CqlError error = CqlError.Deserialize(errorResp);
        return new CassandraException(error.Message.Value, true);
    }

    /// <summary>
    /// A class that handles all the reading.
    /// Allows for multiple execution of queries from the same connection
    /// </summary>
    private async ValueTask HandleReadingAsync(CancellationToken token = default)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                byte[] ackResponse = new byte[CqlFrame.CqlFrameConstSize];
                int amountReceived = 0;
                do
                {
                    amountReceived += await this._socket.ReceiveAsync(ackResponse, token);
                } while (amountReceived < CqlFrame.CqlFrameConstSize);

                CqlFrame frame = CqlFrame.Deserialize(ackResponse);

                if (this._streams.TryRemove(frame.Stream,
                        out TaskCompletionSource<StreamData>? completionSource))
                {
                    if (frame.OpCode == CqlOpCode.Error)
                    {
                        completionSource.SetException(
                            await HandleErrorAsync(frame.Length)
                        );
                        continue;
                    }

                    using ArrayPoolBufferWriter<byte> buffer = new(frame.Length);
                    int bodyAmountReceived = 0;
                    do
                    {
                        int newBodyAmountReceived =
                            await this._socket.ReceiveAsync(
                                buffer.GetMemory(frame.Length - bodyAmountReceived),
                                token
                            );
                        buffer.Advance(newBodyAmountReceived);
                        bodyAmountReceived += newBodyAmountReceived;
                    } while (bodyAmountReceived < frame.Length);

                    StartCompletionSource(frame, buffer.WrittenMemory, completionSource);
                }
                else
                {
                    Console.WriteLine(
                        "Received unknown request. Consuming to not cause issues."
                    );
                    byte[] buffer = new byte[frame.Length];
                    int bodyAmountReceived = 0;
                    do
                    {
                        bodyAmountReceived +=
                            await this._socket.ReceiveAsync(buffer, token);
                    } while (bodyAmountReceived < frame.Length);
                }
            }
        }
        catch (TaskCanceledException)
        {
            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(CancellationToken.None);
                return;
            }
        }
        catch (OperationCanceledException)
        {
            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(CancellationToken.None);
                return;
            }
        }
        catch (SocketException e)
        {
            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(token);
                return;
            }

            if (e.Message != "Operation canceled")
            {
                Console.WriteLine(e);
                throw;
            }
        }
        catch (Exception e)
        {
            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(token);
                return;
            }

            Console.WriteLine($"Reading process failed: {e}");
            await this.DisconnectAsync();
            throw;
        }
    }

    private void StartCompletionSource(
        CqlFrame frame,
        ReadOnlyMemory<byte> buffer,
        TaskCompletionSource<StreamData> completionSource
    )
    {
        ReadOnlySpan<byte> span = buffer.Span;
        CqlStringList? warnings = null;
        if ((frame.Flags & CqlFlags.Warning) != 0)
        {
            warnings = CqlStringList.Deserialize(ref span);
            buffer = buffer[
                warnings.SizeOf()..]; // Warnings should never happen, users should blame themselves for passing warning queries so their fault for this copying
        }

        completionSource.SetResult(new StreamData
        {
            Body = buffer,
            Frame = frame,
            Warnings = warnings
        });
    }

    public void Dispose()
    {
        this.Connected = false;
        this._socket.Dispose();
        this._tokenSource.Cancel();
        GC.SuppressFinalize(this);
    }
}
