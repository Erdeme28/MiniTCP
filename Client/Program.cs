using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Client
{
    public enum PacketType { SYN, SYN_ACK, ACK, DATA }

    public class Packet
    {
        public PacketType Type { get; set; }
        public int SequenceNumber { get; set; }
        public int WindowSize { get; set; }
        public string Payload { get; set; } = string.Empty;

        public byte[] ToBytes() => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this));
        public static Packet FromBytes(byte[] data) => JsonSerializer.Deserialize<Packet>(Encoding.UTF8.GetString(data))!;
    }

    class Program
    {
        static UdpClient udpClient = new UdpClient();
        static IPEndPoint serverEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 11000);
        static int windowSize = 5;
        static int baseSeq = 0;
        static int nextSeq = 0;
        static int totalPackets = 15;

        static ConcurrentDictionary<int, Timer> packetTimers = new ConcurrentDictionary<int, Timer>();
        static ConcurrentDictionary<int, Packet> unackedPackets = new ConcurrentDictionary<int, Packet>();
        static ConcurrentDictionary<int, bool> ackReceived = new ConcurrentDictionary<int, bool>();

        static async Task Main(string[] args)
        {
            Console.WriteLine("=========================================");
            Console.WriteLine("   CLIENT (Selective Repeat)             ");
            Console.WriteLine("=========================================\n");

            Packet syn = new Packet { Type = PacketType.SYN, WindowSize = windowSize };
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("[TX] Sending SYN...");
            Console.ResetColor();
            await SendPacket(syn);

            UdpReceiveResult result = await udpClient.ReceiveAsync();
            Packet synAck = Packet.FromBytes(result.Buffer);

            if (synAck.Type == PacketType.SYN_ACK)
            {
                windowSize = synAck.WindowSize;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"    <- [RX] SYN-ACK received. Negotiated window: {windowSize}");

                Packet ack = new Packet { Type = PacketType.ACK };
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("[TX] Sending ACK. Connection established!\n");
                Console.ResetColor();
                await SendPacket(ack);

                Console.WriteLine("--- STARTING DATA TRANSFER ---\n");

                _ = Task.Run(ListenForAcks);

                while (baseSeq < totalPackets)
                {
                    while (nextSeq < baseSeq + windowSize && nextSeq < totalPackets)
                    {
                        int currentSeq = nextSeq;
                        Packet dataPacket = new Packet { Type = PacketType.DATA, SequenceNumber = currentSeq, Payload = $"File chunk #{currentSeq}" };

                        unackedPackets[currentSeq] = dataPacket;
                        ackReceived[currentSeq] = false;

                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($"[TX] Sent DATA seq {currentSeq}");
                        Console.ResetColor();
                        await SendPacket(dataPacket);

                        packetTimers[currentSeq] = new Timer(async _ => await Retransmit(currentSeq), null, 3000, Timeout.Infinite);

                        nextSeq++;
                        await Task.Delay(600); 
                    }
                    await Task.Delay(100);
                }

                await Task.Delay(4000);
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("\n=========================================");
                Console.WriteLine("[SUCCESS] All packets sent and acknowledged!");
                Console.WriteLine("=========================================");
                Console.ReadLine();
            }
        }

        static async Task ListenForAcks()
        {
            while (true)
            {
                try
                {
                    UdpReceiveResult result = await udpClient.ReceiveAsync();
                    Packet ackPacket = Packet.FromBytes(result.Buffer);

                    if (ackPacket.Type == PacketType.ACK)
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"    <- [RX] Received ACK for seq {ackPacket.SequenceNumber}");
                        Console.ResetColor();
                        ackReceived[ackPacket.SequenceNumber] = true;

                        if (packetTimers.TryRemove(ackPacket.SequenceNumber, out Timer? timer)) timer.Dispose();
                        unackedPackets.TryRemove(ackPacket.SequenceNumber, out _);

                        while (ackReceived.ContainsKey(baseSeq) && ackReceived[baseSeq])
                        {
                            baseSeq++;
                        }
                    }
                }
                catch (Exception) { }
            }
        }

        static async Task Retransmit(int seqNum)
        {
            if (unackedPackets.TryGetValue(seqNum, out Packet? packet))
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"[!] [TIMEOUT] seq {seqNum} expired! STRICTLY retransmitting...");
                Console.ResetColor();

                await SendPacket(packet);

                if (packetTimers.TryGetValue(seqNum, out Timer? timer)) timer.Change(3000, Timeout.Infinite);
            }
        }

        static async Task SendPacket(Packet packet)
        {
            byte[] data = packet.ToBytes();
            await udpClient.SendAsync(data, data.Length, serverEndpoint);
        }
    }
}