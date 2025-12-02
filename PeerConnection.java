import java.io.*;
import java.net.*;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages a connection to a single peer
 */
public class PeerConnection {
    private int peerId;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private AtomicBoolean isChoked;
    private AtomicBoolean isInterested;
    private AtomicBoolean peerIsChoked;
    private AtomicBoolean peerIsInterested;
    private BitSet peerBitfield;
    private int numberOfPieces;
    private AtomicLong downloadRate; // Bytes downloaded in current interval
    private long lastRateResetTime;
    private Logger logger;
    private FileManager fileManager;
    private int myPeerId;

    public PeerConnection(int myPeerId, int peerId, Socket socket, int numberOfPieces, Logger logger, FileManager fileManager) throws IOException {
        this(myPeerId, peerId, socket, new DataInputStream(socket.getInputStream()), 
             new DataOutputStream(socket.getOutputStream()), numberOfPieces, logger, fileManager);
    }

    public PeerConnection(int myPeerId, int peerId, Socket socket, DataInputStream inputStream, 
                         DataOutputStream outputStream, int numberOfPieces, Logger logger, FileManager fileManager) throws IOException {
        this.myPeerId = myPeerId;
        this.peerId = peerId;
        this.socket = socket;
        this.numberOfPieces = numberOfPieces;
        this.logger = logger;
        this.fileManager = fileManager;
        
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        
        this.isChoked = new AtomicBoolean(true);
        this.isInterested = new AtomicBoolean(false);
        this.peerIsChoked = new AtomicBoolean(true);
        this.peerIsInterested = new AtomicBoolean(false);
        this.peerBitfield = new BitSet(numberOfPieces);
        this.downloadRate = new AtomicLong(0);
        this.lastRateResetTime = System.currentTimeMillis();
    }

    /**
     * Send handshake message
     */
    public void sendHandshake(int peerId) throws IOException {
        byte[] handshake = Message.createHandshake(peerId);
        outputStream.write(handshake);
        outputStream.flush();
    }

    /**
     * Receive handshake message
     */
    public int receiveHandshake() throws IOException {
        byte[] handshake = new byte[32];
        int bytesRead = 0;
        while (bytesRead < 32) {
            int read = inputStream.read(handshake, bytesRead, 32 - bytesRead);
            if (read == -1) {
                throw new IOException("Connection closed during handshake");
            }
            bytesRead += read;
        }
        return Message.parseHandshake(handshake);
    }

    /**
     * Send a message
     */
    public void sendMessage(Message message) throws IOException {
        byte[] data = message.toByteArray();
        outputStream.write(data);
        outputStream.flush();
    }

    /**
     * Receive a message
     */
    public Message receiveMessage() throws IOException {
        return Message.readMessage(inputStream);
    }

    /**
     * Send bitfield
     */
    public void sendBitfield(BitSet bitfield) throws IOException {
        Message bitfieldMsg = Message.createBitfieldMessage(bitfield, numberOfPieces);
        sendMessage(bitfieldMsg);
    }

    /**
     * Receive bitfield
     */
    public void receiveBitfield() throws IOException {
        Message message = receiveMessage();
        if (message.getMessageType() != Message.BITFIELD) {
            throw new IOException("Expected bitfield message");
        }
        peerBitfield = Message.parseBitfieldMessage(message.getPayload(), numberOfPieces);
        updateInterest();
    }

    /**
     * Send interested message
     */
    public void sendInterested() throws IOException {
        if (!isInterested.get()) {
            sendMessage(new Message(Message.INTERESTED, null));
            isInterested.set(true);
        }
    }

    /**
     * Send not interested message
     */
    public void sendNotInterested() throws IOException {
        if (isInterested.get()) {
            sendMessage(new Message(Message.NOT_INTERESTED, null));
            isInterested.set(false);
        }
    }

    /**
     * Send choke message
     */
    public void sendChoke() throws IOException {
        if (!peerIsChoked.get()) {
            sendMessage(new Message(Message.CHOKE, null));
            peerIsChoked.set(true);
        }
    }

    /**
     * Send unchoke message
     */
    public void sendUnchoke() throws IOException {
        if (peerIsChoked.get()) {
            sendMessage(new Message(Message.UNCHOKE, null));
            peerIsChoked.set(false);
            logger.logUnchoked(peerId);
        }
    }

    /**
     * Send have message
     */
    public void sendHave(int pieceIndex) throws IOException {
        Message haveMsg = Message.createHaveMessage(pieceIndex);
        sendMessage(haveMsg);
    }

    /**
     * Send request message
     */
    public void sendRequest(int pieceIndex) throws IOException {
        Message requestMsg = Message.createRequestMessage(pieceIndex);
        sendMessage(requestMsg);
    }

    /**
     * Send piece message
     */
    public void sendPiece(int pieceIndex, byte[] pieceData) throws IOException {
        Message pieceMsg = Message.createPieceMessage(pieceIndex, pieceData);
        sendMessage(pieceMsg);
        // Note: downloadRate tracks bytes we downloaded FROM this peer, not bytes we uploaded TO them
    }

    /**
     * Update interest based on peer's bitfield
     */
    public void updateInterest() throws IOException {
        BitSet myBitfield = fileManager.getBitfield();
        boolean hasInterestingPieces = false;
        
        for (int i = 0; i < numberOfPieces; i++) {
            if (peerBitfield.get(i) && !myBitfield.get(i)) {
                hasInterestingPieces = true;
                break;
            }
        }
        
        if (hasInterestingPieces) {
            sendInterested();
        } else {
            sendNotInterested();
        }
    }

    /**
     * Check if peer has interesting pieces
     */
    public boolean hasInterestingPieces() {
        BitSet myBitfield = fileManager.getBitfield();
        for (int i = 0; i < numberOfPieces; i++) {
            if (peerBitfield.get(i) && !myBitfield.get(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get a random piece that peer has but we don't
     */
    public int getRandomInterestingPiece() {
        BitSet myBitfield = fileManager.getBitfield();
        java.util.List<Integer> interestingPieces = new java.util.ArrayList<>();
        
        for (int i = 0; i < numberOfPieces; i++) {
            if (peerBitfield.get(i) && !myBitfield.get(i)) {
                interestingPieces.add(i);
            }
        }
        
        if (interestingPieces.isEmpty()) {
            return -1;
        }
        
        return interestingPieces.get((int) (Math.random() * interestingPieces.size()));
    }

    // Getters and setters
    public int getPeerId() { return peerId; }
    public boolean isChoked() { return isChoked.get(); }
    public void setChoked(boolean choked) { this.isChoked.set(choked); }
    public boolean isInterested() { return isInterested.get(); }
    public boolean peerIsChoked() { return peerIsChoked.get(); }
    public boolean peerIsInterested() { return peerIsInterested.get(); }
    public void setPeerIsInterested(boolean interested) { this.peerIsInterested.set(interested); }
    public BitSet getPeerBitfield() { return (BitSet) peerBitfield.clone(); }
    public void updatePeerBitfield(int pieceIndex) { peerBitfield.set(pieceIndex); }
    public long getDownloadRate() { return downloadRate.get(); }
    public void addDownloadRate(long bytes) {
        downloadRate.addAndGet(bytes);
    }
    public void resetDownloadRate() { 
        downloadRate.set(0);
        lastRateResetTime = System.currentTimeMillis();
    }
    public long getLastRateResetTime() { return lastRateResetTime; }

    /**
     * Handle received have message
     */
    public void handleHaveMessage() throws IOException {
        Message message = receiveMessage();
        if (message.getMessageType() != Message.HAVE) {
            throw new IOException("Expected have message");
        }
        int pieceIndex = Message.parseHaveMessage(message.getPayload());
        peerBitfield.set(pieceIndex);
        logger.logReceivedHave(peerId, pieceIndex);
        updateInterest();
    }

    /**
     * Handle received interested/not interested message
     */
    public void handleInterestMessage(byte messageType) {
        if (messageType == Message.INTERESTED) {
            peerIsInterested.set(true);
            logger.logReceivedInterested(peerId);
        } else if (messageType == Message.NOT_INTERESTED) {
            peerIsInterested.set(false);
            logger.logReceivedNotInterested(peerId);
        }
    }

    /**
     * Handle received choke/unchoke message
     */
    public void handleChokeMessage(byte messageType) {
        if (messageType == Message.CHOKE) {
            isChoked.set(true);
            logger.logChoked(peerId);
        } else if (messageType == Message.UNCHOKE) {
            isChoked.set(false);
            logger.logUnchoked(peerId);
        }
    }

    /**
     * Close the connection
     */
    public void close() throws IOException {
        if (inputStream != null) inputStream.close();
        if (outputStream != null) outputStream.close();
        if (socket != null) socket.close();
    }
}

