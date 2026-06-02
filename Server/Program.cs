using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Server
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

    public static class NetworkSimulator
    {
        private static Random rand = new Random();
        public static bool ShouldDropPacket() => rand.NextDouble() < 0.15; 
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=========================================");
            Console.WriteLine("   SERVER (Selective Repeat)             ");
            Console.WriteLine("=========================================\n");

            UdpClient udpServer = new UdpClient(11000);
            int windowSize = 5;
            Dictionary<int, string> receiveBuffer = new Dictionary<int, string>();
            int expectedSeqNum = 0;

            Console.WriteLine("Listening on port 11000...\n");

            while (true)
            {
                UdpReceiveResult result = await udpServer.ReceiveAsync();
                Packet packet = Packet.FromBytes(result.Buffer);

                if (packet.Type == PacketType.SYN)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"    <- [RX] SYN received. Requested window: {packet.WindowSize}");
                    windowSize = Math.Min(windowSize, packet.WindowSize);

                    Packet synAck = new Packet { Type = PacketType.SYN_ACK, WindowSize = windowSize };
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"[TX] Sending SYN-ACK. Accepted window: {windowSize}");
                    Console.ResetColor();

                    byte[] data = synAck.ToBytes();
                    await udpServer.SendAsync(data, data.Length, result.RemoteEndPoint);
                }
                else if (packet.Type == PacketType.DATA)
                {
                    if (NetworkSimulator.ShouldDropPacket())
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[x] [LOSS SIMULATION] DATA seq {packet.SequenceNumber} dropped on the way!");
                        Console.ResetColor();
                        continue;
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"    <- [RX] Received DATA seq {packet.SequenceNumber} | {packet.Payload}");

                    Packet ack = new Packet { Type = PacketType.ACK, SequenceNumber = packet.SequenceNumber };
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"[TX] Sending ACK for seq {packet.SequenceNumber}");
                    Console.ResetColor();

                    byte[] ackData = ack.ToBytes();
                    await udpServer.SendAsync(ackData, ackData.Length, result.RemoteEndPoint);

                    if (packet.SequenceNumber >= expectedSeqNum && packet.SequenceNumber < expectedSeqNum + windowSize)
                    {
                        if (!receiveBuffer.ContainsKey(packet.SequenceNumber))
                        {
                            receiveBuffer[packet.SequenceNumber] = packet.Payload;
                        }

                        while (receiveBuffer.ContainsKey(expectedSeqNum))
                        {
                            Console.ForegroundColor = ConsoleColor.Magenta;
                            Console.WriteLine($"    => [ASSEMBLY] Delivered to app: {receiveBuffer[expectedSeqNum]}");
                            Console.ResetColor();
                            receiveBuffer.Remove(expectedSeqNum);
                            expectedSeqNum++;
                        }
                    }
                }
            }
        }
    }
}