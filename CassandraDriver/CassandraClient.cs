using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Request;
using CassandraDriver.Frames.Response;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

public class CassandraClient : IDisposable
{
    private readonly Socket _socket;
    private readonly string _host;
    private readonly int _port;
    private readonly CancellationTokenSource _tokenSource = new();

    private readonly ConcurrentDictionary<short, TaskCompletionSource<StreamData>>
        _streams = new();

    public bool Connected { get; private set; } = false;
    public string? DefaultKeyspace { get; set; }

    public CassandraClient(string host, int port = 9042, string? defaultKeyspace = null)
    {
        _socket = new Socket(AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);
        this._host = host;
        this._port = port;
        DefaultKeyspace = defaultKeyspace;
    }

    public async Task ConnectAsync()
    {
        await this._socket.ConnectAsync(this._host, this._port);
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

        Connected = true;
        if (DefaultKeyspace is not null)
        {
            _ = QueryAsync("USE " + DefaultKeyspace);
        }

        _ = Task.Run(() => HandleReadingAsync());
        await Task.Delay(TimeSpan.FromSeconds(1));
    }

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

        await this._socket.SendAsync(writer.WrittenMemory);

        TaskCompletionSource<StreamData> completionSource = new();
        this._streams.TryAdd(stream, completionSource);
        StreamData data = await completionSource.Task;


        if (data.Frame.OpCode != CqlOpCode.Result)
        {
            throw new CassandraException("Didn't get result opcode");
        }

        return Query.Deserialize(data.Body, data.Warnings);
    }

    public async Task DisconnectAsync()
    {
        await this._tokenSource.CancelAsync();
        await this._socket.DisconnectAsync(true);
        Connected = false;
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
                            await HandleErrorAsync(frame.Length));
                    }

                    byte[] buffer = new byte[frame.Length];
                    int bodyAmountReceived = 0;
                    do
                    {
                        bodyAmountReceived +=
                            await this._socket.ReceiveAsync(buffer, token);
                    } while (bodyAmountReceived < frame.Length);

                    StartCompletionSource(frame, buffer, completionSource);
                }
            }
        }
        catch (TaskCanceledException)
        {
            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(token);
                return;
            }
        }
        catch (SocketException e)
        {
            if (e.Message != "Operation canceled")
            {
                ExceptionDispatchInfo.Capture(e).Throw(); // Helps with debugging
            }

            foreach ((short _, TaskCompletionSource<StreamData> source) in this._streams)
            {
                source.SetCanceled(token);
                return;
            }
        }
    }

    private void StartCompletionSource(
        CqlFrame frame,
        byte[] buffer,
        TaskCompletionSource<StreamData> completionSource
    )
    {
        ReadOnlySpan<byte> span = buffer;
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
        Connected = false;
        this._socket.Dispose();
        this._tokenSource.Cancel();
    }
}
