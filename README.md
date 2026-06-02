# Reliable UDP (Selective Repeat ARQ)

A custom Transport Layer protocol built in C# that introduces TCP-like reliability mechanisms over an inherently unreliable UDP channel. This project is an academic implementation of the **Selective Repeat Automatic Repeat reQuest (ARQ)** sliding window protocol.

## Protocol Architecture

By default, UDP does not guarantee packet delivery, ordering, or duplicate protection. This application solves these issues programmatically by implementing:

1. **Custom Handshake (SYN / SYN-ACK / ACK):**
   - The protocol initiates communication with a 3-way handshake to negotiate parameters such as the initial Window Size between the sender and receiver.

2. **Selective Repeat Sliding Window:**
   - Instead of discarding all packets after a lost frame (like Go-Back-N), this implementation buffers out-of-order packets.
   - Only explicitly unacknowledged (lost) packets are retransmitted, making it highly efficient.

3. **Concurrency & Thread Safety:**
   - Utilizes `ConcurrentDictionary` and asynchronous Tasks (`async/await`) to handle multiple timers and packet acknowledgments concurrently without race conditions.

4. **Independent Timers:**
   - Every transmitted packet has its own independent timeout mechanism (`System.Threading.Timer`). If an ACK is not received within the threshold, that specific packet is strictly retransmitted.

## Loss Simulation Engine

To demonstrate the protocol's robustness, the Server includes a custom `NetworkSimulator`.
- This engine artificially drops incoming packets with a 15% probability (rand.NextDouble() < 0.15). 
- When a drop occurs, the Client's independent timer expires, triggering a targeted retransmission of the lost sequence number, which the Server then successfully buffers and reassembles.

## Technical Details

- **Language & Framework:** C# / .NET
- **Networking:** Asynchronous UDP Sockets (UdpClient).
- **Data Serialization:** Packets are wrapped in a custom Packet class (containing Sequence Numbers, Window Sizes, and Payload) and serialized to JSON bytes for network transport.
