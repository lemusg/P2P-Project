import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Message types and utilities for P2P protocol
 */
public class Message {
    // Message type constants
    public static final byte CHOKE = 0;
    public static final byte UNCHOKE = 1;
    public static final byte INTERESTED = 2;
    public static final byte NOT_INTERESTED = 3;
    public static final byte HAVE = 4;
    public static final byte BITFIELD = 5;
    public static final byte REQUEST = 6;
    public static final byte PIECE = 7;

    private byte messageType;
    private byte[] payload;

    public Message(byte messageType, byte[] payload) {
        this.messageType = messageType;
        this.payload = payload != null ? payload : new byte[0];
    }

    public byte getMessageType() {
        return messageType;
    }

    public byte[] getPayload() {
        return payload;
    }

    /**
     * Convert message to byte array for transmission
     */
    public byte[] toByteArray() {
        int messageLength = 1 + payload.length; // 1 byte for message type + payload
        ByteBuffer buffer = ByteBuffer.allocate(4 + messageLength);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageLength);
        buffer.put(messageType);
        if (payload.length > 0) {
            buffer.put(payload);
        }
        return buffer.array();
    }

    /**
     * Read a message from input stream
     */
    public static Message readMessage(InputStream in) throws IOException {
        // Read 4-byte message length
        byte[] lengthBytes = new byte[4];
        int bytesRead = 0;
        while (bytesRead < 4) {
            int read = in.read(lengthBytes, bytesRead, 4 - bytesRead);
            if (read == -1) {
                throw new IOException("Connection closed");
            }
            bytesRead += read;
        }
        
        int messageLength = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        
        // Read 1-byte message type
        int type = in.read();
        if (type == -1) {
            throw new IOException("Connection closed");
        }
        byte messageType = (byte) type;
        
        // Read payload
        byte[] payload = new byte[messageLength - 1];
        if (payload.length > 0) {
            bytesRead = 0;
            while (bytesRead < payload.length) {
                int read = in.read(payload, bytesRead, payload.length - bytesRead);
                if (read == -1) {
                    throw new IOException("Connection closed");
                }
                bytesRead += read;
            }
        }
        
        return new Message(messageType, payload);
    }

    /**
     * Create handshake message
     */
    public static byte[] createHandshake(int peerId) {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        // 18-byte handshake header
        String header = "P2PFILESHARINGPROJ";
        buffer.put(header.getBytes());
        
        // 10-byte zero bits
        buffer.put(new byte[10]);
        
        // 4-byte peer ID
        buffer.putInt(peerId);
        
        return buffer.array();
    }

    /**
     * Parse handshake message
     */
    public static int parseHandshake(byte[] handshake) throws IOException {
        if (handshake.length != 32) {
            throw new IOException("Invalid handshake length");
        }
        
        // Check header
        String header = new String(handshake, 0, 18);
        if (!header.equals("P2PFILESHARINGPROJ")) {
            throw new IOException("Invalid handshake header");
        }
        
        // Extract peer ID (bytes 28-31)
        ByteBuffer buffer = ByteBuffer.wrap(handshake, 28, 4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.getInt();
    }

    /**
     * Create bitfield message from BitSet
     */
    public static Message createBitfieldMessage(BitSet bitfield, int numberOfPieces) {
        int bitfieldLength = (numberOfPieces + 7) / 8; // Round up to nearest byte
        byte[] bitfieldBytes = new byte[bitfieldLength];
        
        for (int i = 0; i < numberOfPieces; i++) {
            if (bitfield.get(i)) {
                int byteIndex = i / 8;
                int bitIndex = 7 - (i % 8); // High bit to low bit
                bitfieldBytes[byteIndex] |= (1 << bitIndex);
            }
        }
        
        return new Message(BITFIELD, bitfieldBytes);
    }

    /**
     * Parse bitfield message to BitSet
     */
    public static BitSet parseBitfieldMessage(byte[] payload, int numberOfPieces) {
        BitSet bitfield = new BitSet(numberOfPieces);
        
        for (int i = 0; i < numberOfPieces; i++) {
            int byteIndex = i / 8;
            if (byteIndex < payload.length) {
                int bitIndex = 7 - (i % 8); // High bit to low bit
                if ((payload[byteIndex] & (1 << bitIndex)) != 0) {
                    bitfield.set(i);
                }
            }
        }
        
        return bitfield;
    }

    /**
     * Create have message
     */
    public static Message createHaveMessage(int pieceIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(pieceIndex);
        return new Message(HAVE, buffer.array());
    }

    /**
     * Parse have message
     */
    public static int parseHaveMessage(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.getInt();
    }

    /**
     * Create request message
     */
    public static Message createRequestMessage(int pieceIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(pieceIndex);
        return new Message(REQUEST, buffer.array());
    }

    /**
     * Parse request message
     */
    public static int parseRequestMessage(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.getInt();
    }

    /**
     * Create piece message
     */
    public static Message createPieceMessage(int pieceIndex, byte[] pieceData) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + pieceData.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(pieceIndex);
        buffer.put(pieceData);
        return new Message(PIECE, buffer.array());
    }

    /**
     * Parse piece message
     */
    public static PieceData parsePieceMessage(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.order(ByteOrder.BIG_ENDIAN);
        int pieceIndex = buffer.getInt();
        byte[] pieceData = new byte[payload.length - 4];
        buffer.get(pieceData);
        return new PieceData(pieceIndex, pieceData);
    }

    /**
     * Helper class for piece data
     */
    public static class PieceData {
        private int pieceIndex;
        private byte[] data;

        public PieceData(int pieceIndex, byte[] data) {
            this.pieceIndex = pieceIndex;
            this.data = data;
        }

        public int getPieceIndex() { return pieceIndex; }
        public byte[] getData() { return data; }
    }
}

