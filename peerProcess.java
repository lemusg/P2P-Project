import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Main peer process for P2P file sharing
 */
public class peerProcess {
    private int peerId;
    private CommonConfig commonConfig;
    private List<PeerInfo> allPeers;
    private PeerInfo myPeerInfo;
    private FileManager fileManager;
    private Logger logger;
    private Map<Integer, PeerConnection> connections;
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private volatile boolean running;
    private int numberOfPreferredNeighbors;
    private int unchokingInterval;
    private int optimisticUnchokingInterval;
    private Set<Integer> preferredNeighbors;
    private Integer optimisticallyUnchokedNeighbor;
    private ScheduledExecutorService scheduler;
    private Map<Integer, Integer> requestedPieces; // peerId -> pieceIndex

    public peerProcess(int peerId) {
        this.peerId = peerId;
        this.connections = new ConcurrentHashMap<>();
        this.running = true;
        this.preferredNeighbors = new HashSet<>();
        this.executorService = Executors.newCachedThreadPool();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.requestedPieces = new ConcurrentHashMap<>();
    }

    public void start() {
        try {
            // Read configuration files
            String workingDir = System.getProperty("user.dir");
            String commonConfigPath = workingDir + File.separator + "Common.cfg";
            String peerInfoPath = workingDir + File.separator + "PeerInfo.cfg";
            
            commonConfig = new CommonConfig(commonConfigPath);
            allPeers = PeerInfoReader.readPeerInfo(peerInfoPath);
            
            // Find my peer info
            myPeerInfo = null;
            for (PeerInfo peer : allPeers) {
                if (peer.getPeerId() == peerId) {
                    myPeerInfo = peer;
                    break;
                }
            }
            
            if (myPeerInfo == null) {
                System.err.println("Peer ID " + peerId + " not found in PeerInfo.cfg");
                return;
            }
            
            numberOfPreferredNeighbors = commonConfig.getNumberOfPreferredNeighbors();
            unchokingInterval = commonConfig.getUnchokingInterval();
            optimisticUnchokingInterval = commonConfig.getOptimisticUnchokingInterval();
            
            // Initialize file manager
            String peerDirectory = workingDir + File.separator + peerId;
            fileManager = new FileManager(peerDirectory, commonConfig.getFileName(), 
                                        commonConfig.getPieceSize(), commonConfig.getFileSize(), 
                                        myPeerInfo.hasFile());
            
            // Initialize logger
            String logPath = workingDir + File.separator + "log_" + peerId + ".log";
            logger = new Logger(peerId, logPath);
            
            System.out.println("Peer " + peerId + " starting...");
            System.out.println("Has file: " + myPeerInfo.hasFile());
            System.out.println("Listening on: " + myPeerInfo.getHostName() + ":" + myPeerInfo.getListeningPort());
            
            // Start server socket to accept incoming connections
            startServer();
            
            // Connect to peers that started before this peer
            connectToPreviousPeers();
            
            // Wait a bit for all connections to be established
            Thread.sleep(2000);
            
            // Start message handlers for each connection
            startMessageHandlers();
            
            // Start choking/unchoking scheduler
            startChokingScheduler();
            
            // Start optimistic unchoking scheduler
            startOptimisticUnchokingScheduler();
            
            // Start piece request handler (for downloading)
            startPieceRequestHandler();
            
            // Monitor for completion
            monitorCompletion();
            
        } catch (Exception e) {
            System.err.println("Error starting peer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void startServer() throws IOException {
        serverSocket = new ServerSocket(myPeerInfo.getListeningPort());
        executorService.submit(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    executorService.submit(() -> handleIncomingConnection(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Error accepting connection: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void handleIncomingConnection(Socket socket) {
        try {
            // Receive handshake first
            DataInputStream tempIn = new DataInputStream(socket.getInputStream());
            DataOutputStream tempOut = new DataOutputStream(socket.getOutputStream());
            
            byte[] handshake = new byte[32];
            int bytesRead = 0;
            while (bytesRead < 32) {
                int read = tempIn.read(handshake, bytesRead, 32 - bytesRead);
                if (read == -1) {
                    socket.close();
                    return;
                }
                bytesRead += read;
            }
            
            int otherPeerId = Message.parseHandshake(handshake);
            
            // Verify peer ID
            PeerInfo otherPeer = null;
            for (PeerInfo peer : allPeers) {
                if (peer.getPeerId() == otherPeerId) {
                    otherPeer = peer;
                    break;
                }
            }
            
            if (otherPeer == null) {
                System.err.println("Unknown peer ID: " + otherPeerId);
                socket.close();
                return;
            }
            
            // Send handshake
            byte[] myHandshake = Message.createHandshake(peerId);
            tempOut.write(myHandshake);
            tempOut.flush();
            
            // Create connection using the existing streams
            PeerConnection connection = new PeerConnection(peerId, otherPeerId, socket, 
                                          tempIn, tempOut, commonConfig.getNumberOfPieces(), 
                                          logger, fileManager);
            
            logger.logTcpConnectionReceived(otherPeerId);
            
            // Exchange bitfields
            if (fileManager.getNumberOfPieces() > 0) {
                connection.sendBitfield(fileManager.getBitfield());
            }
            
            // Receive bitfield if peer has pieces (with timeout)
            try {
                // Set a short timeout for bitfield
                socket.setSoTimeout(5000);
                connection.receiveBitfield();
                socket.setSoTimeout(0); // Remove timeout
            } catch (IOException e) {
                // Peer may not have sent bitfield (has no pieces)
                socket.setSoTimeout(0); // Remove timeout
            }
            
            // Update interest
            connection.updateInterest();
            
            connections.put(otherPeerId, connection);
            
            // Update interest
            connection.updateInterest();
            
            // Start message handler for this connection
            final PeerConnection conn = connection;
            executorService.submit(() -> {
                while (running) {
                    try {
                        Message message = conn.receiveMessage();
                        handleMessage(conn, message);
                    } catch (IOException e) {
                        if (running) {
                            System.err.println("Error receiving message from peer " + conn.getPeerId() + ": " + e.getMessage());
                        }
                        break;
                    }
                }
            });
            
        } catch (IOException e) {
            System.err.println("Error handling incoming connection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void connectToPreviousPeers() throws IOException {
        for (PeerInfo peer : allPeers) {
            if (peer.getPeerId() < peerId) {
                // Connect to this peer
                try {
                    Socket socket = new Socket(peer.getHostName(), peer.getListeningPort());
                    PeerConnection connection = new PeerConnection(peerId, peer.getPeerId(), socket, 
                                                                  commonConfig.getNumberOfPieces(), 
                                                                  logger, fileManager);
                    
                    // Exchange handshakes
                    connection.sendHandshake(peerId);
                    int receivedPeerId = connection.receiveHandshake();
                    
                    if (receivedPeerId != peer.getPeerId()) {
                        System.err.println("Peer ID mismatch: expected " + peer.getPeerId() + ", got " + receivedPeerId);
                        connection.close();
                        continue;
                    }
                    
                    logger.logTcpConnectionMade(peer.getPeerId());
                    
                    // Send bitfield if we have pieces
                    if (fileManager.getNumberOfPieces() > 0) {
                        connection.sendBitfield(fileManager.getBitfield());
                    }
                    
                    // Receive bitfield if peer has pieces
                    try {
                        connection.receiveBitfield();
                    } catch (IOException e) {
                        // Peer may not have sent bitfield
                    }
                    
                    // Update interest
                    connection.updateInterest();
                    
                    connections.put(peer.getPeerId(), connection);
                    
                } catch (ConnectException e) {
                    System.err.println("Could not connect to peer " + peer.getPeerId() + ": " + e.getMessage());
                }
            }
        }
    }

    private void startMessageHandlers() {
        for (PeerConnection connection : connections.values()) {
            final PeerConnection conn = connection;
            executorService.submit(() -> {
                while (running) {
                    try {
                        Message message = conn.receiveMessage();
                        handleMessage(conn, message);
                    } catch (IOException e) {
                        if (running) {
                            System.err.println("Error receiving message from peer " + conn.getPeerId() + ": " + e.getMessage());
                        }
                        break;
                    }
                }
            });
        }
    }

    private void handleMessage(PeerConnection connection, Message message) throws IOException {
        byte messageType = message.getMessageType();
        
        switch (messageType) {
            case Message.CHOKE:
                connection.handleChokeMessage(Message.CHOKE);
                break;
            case Message.UNCHOKE:
                connection.handleChokeMessage(Message.UNCHOKE);
                break;
            case Message.INTERESTED:
                connection.handleInterestMessage(Message.INTERESTED);
                break;
            case Message.NOT_INTERESTED:
                connection.handleInterestMessage(Message.NOT_INTERESTED);
                break;
            case Message.HAVE:
                connection.handleHaveMessage();
                break;
            case Message.REQUEST:
                handleRequestMessage(connection, message);
                break;
            case Message.PIECE:
                handlePieceMessage(connection, message);
                break;
        }
    }

    private void handleRequestMessage(PeerConnection connection, Message message) throws IOException {
        // Only send piece if peer is unchoked
        if (!connection.peerIsChoked()) {
            int pieceIndex = Message.parseRequestMessage(message.getPayload());
            
            if (fileManager.hasPiece(pieceIndex)) {
                byte[] pieceData = fileManager.readPiece(pieceIndex);
                connection.sendPiece(pieceIndex, pieceData);
            }
        }
    }

    private void handlePieceMessage(PeerConnection connection, Message message) throws IOException {
        Message.PieceData pieceData = Message.parsePieceMessage(message.getPayload());
        int pieceIndex = pieceData.getPieceIndex();
        byte[] data = pieceData.getData();
        
        if (!fileManager.hasPiece(pieceIndex)) {
            fileManager.writePiece(pieceIndex, data);
            int numPieces = fileManager.getNumberOfPieces();
            logger.logDownloadedPiece(pieceIndex, connection.getPeerId(), numPieces);
            
            // Update download rate (bytes we downloaded from this peer)
            connection.addDownloadRate(data.length);
            
            // Clear requested piece for this peer so we can request another one
            requestedPieces.remove(connection.getPeerId());
            
            // Send have message to all connections
            for (PeerConnection conn : connections.values()) {
                if (conn != connection) {
                    conn.sendHave(pieceIndex);
                }
            }
            
            // Update interest for all connections
            for (PeerConnection conn : connections.values()) {
                conn.updateInterest();
            }
            
            // Check if file is complete
            if (fileManager.isFileComplete()) {
                logger.logDownloadComplete();
            }
        }
    }

    private void startChokingScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                updatePreferredNeighbors();
            } catch (Exception e) {
                System.err.println("Error updating preferred neighbors: " + e.getMessage());
            }
        }, unchokingInterval, unchokingInterval, TimeUnit.SECONDS);
    }

    private void updatePreferredNeighbors() throws IOException {
        // Calculate download rates and select preferred neighbors
        List<PeerConnection> interestedConnections = new ArrayList<>();
        for (PeerConnection conn : connections.values()) {
            if (conn.peerIsInterested()) {
                interestedConnections.add(conn);
            }
        }
        
        if (interestedConnections.isEmpty()) {
            return;
        }
        
        // If we have complete file, select randomly
        if (fileManager.isFileComplete()) {
            Collections.shuffle(interestedConnections);
            int count = Math.min(numberOfPreferredNeighbors, interestedConnections.size());
            Set<Integer> newPreferred = new HashSet<>();
            for (int i = 0; i < count; i++) {
                newPreferred.add(interestedConnections.get(i).getPeerId());
            }
            setPreferredNeighbors(newPreferred);
        } else {
            // Sort by download rate
            interestedConnections.sort((a, b) -> Long.compare(b.getDownloadRate(), a.getDownloadRate()));
            
            int count = Math.min(numberOfPreferredNeighbors, interestedConnections.size());
            Set<Integer> newPreferred = new HashSet<>();
            for (int i = 0; i < count; i++) {
                newPreferred.add(interestedConnections.get(i).getPeerId());
            }
            setPreferredNeighbors(newPreferred);
        }
        
        // Reset download rates
        for (PeerConnection conn : connections.values()) {
            conn.resetDownloadRate();
        }
    }

    private void setPreferredNeighbors(Set<Integer> newPreferred) throws IOException {
        // Choke peers that are no longer preferred (unless they're optimistically unchoked)
        for (PeerConnection conn : connections.values()) {
            int peerId = conn.getPeerId();
            boolean shouldBeUnchoked = newPreferred.contains(peerId) || 
                                     (optimisticallyUnchokedNeighbor != null && 
                                      optimisticallyUnchokedNeighbor == peerId);
            
            if (!shouldBeUnchoked && !conn.peerIsChoked()) {
                conn.sendChoke();
            } else if (shouldBeUnchoked && conn.peerIsChoked()) {
                conn.sendUnchoke();
            }
        }
        
        preferredNeighbors = newPreferred;
        
        // Log preferred neighbors
        int[] preferredArray = newPreferred.stream().mapToInt(i -> i).toArray();
        logger.logPreferredNeighborsChanged(preferredArray);
    }

    private void startOptimisticUnchokingScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                updateOptimisticallyUnchokedNeighbor();
            } catch (Exception e) {
                System.err.println("Error updating optimistically unchoked neighbor: " + e.getMessage());
            }
        }, optimisticUnchokingInterval, optimisticUnchokingInterval, TimeUnit.SECONDS);
    }

    private void updateOptimisticallyUnchokedNeighbor() throws IOException {
        // Find choked but interested peers
        List<PeerConnection> candidates = new ArrayList<>();
        for (PeerConnection conn : connections.values()) {
            if (conn.peerIsChoked() && conn.peerIsInterested() && 
                !preferredNeighbors.contains(conn.getPeerId())) {
                candidates.add(conn);
            }
        }
        
        if (candidates.isEmpty()) {
            optimisticallyUnchokedNeighbor = null;
            return;
        }
        
        // Select randomly
        PeerConnection selected = candidates.get((int) (Math.random() * candidates.size()));
        int newOptimistic = selected.getPeerId();
        
        // If different from current, update
        if (optimisticallyUnchokedNeighbor == null || optimisticallyUnchokedNeighbor != newOptimistic) {
            // Choke old optimistic neighbor if it's not preferred
            if (optimisticallyUnchokedNeighbor != null) {
                PeerConnection oldConn = connections.get(optimisticallyUnchokedNeighbor);
                if (oldConn != null && !preferredNeighbors.contains(optimisticallyUnchokedNeighbor)) {
                    oldConn.sendChoke();
                }
            }
            
            optimisticallyUnchokedNeighbor = newOptimistic;
            selected.sendUnchoke();
            logger.logOptimisticallyUnchokedNeighborChanged(newOptimistic);
        }
    }

    private void startPieceRequestHandler() {
        executorService.submit(() -> {
            while (running) {
                try {
                    for (PeerConnection conn : connections.values()) {
                        if (!conn.isChoked() && conn.hasInterestingPieces()) {
                            int peerId = conn.getPeerId();
                            
                            // Check if we already requested a piece from this peer
                            if (!requestedPieces.containsKey(peerId)) {
                                int pieceIndex = conn.getRandomInterestingPiece();
                                if (pieceIndex != -1) {
                                    // Check if we haven't requested this piece from another peer
                                    boolean alreadyRequested = requestedPieces.containsValue(pieceIndex);
                                    if (!alreadyRequested && !fileManager.hasPiece(pieceIndex)) {
                                        conn.sendRequest(pieceIndex);
                                        requestedPieces.put(peerId, pieceIndex);
                                    }
                                }
                            }
                        } else {
                            // Remove from requested if choked
                            requestedPieces.remove(conn.getPeerId());
                        }
                    }
                    
                    Thread.sleep(100); // Small delay to avoid busy waiting
                } catch (Exception e) {
                    if (running) {
                        System.err.println("Error in piece request handler: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void monitorCompletion() {
        executorService.submit(() -> {
            while (running) {
                try {
                    // Check if all peers have complete file
                    boolean allComplete = true;
                    for (PeerConnection conn : connections.values()) {
                        BitSet peerBitfield = conn.getPeerBitfield();
                        if (peerBitfield.cardinality() < commonConfig.getNumberOfPieces()) {
                            allComplete = false;
                            break;
                        }
                    }
                    
                    // Also check if we have complete file
                    if (!fileManager.isFileComplete()) {
                        allComplete = false;
                    }
                    
                    if (allComplete && connections.size() > 0) {
                        System.out.println("All peers have downloaded the complete file. Terminating...");
                        shutdown();
                        break;
                    }
                    
                    Thread.sleep(5000); // Check every 5 seconds
                } catch (Exception e) {
                    if (running) {
                        System.err.println("Error in completion monitor: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void shutdown() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            
            for (PeerConnection conn : connections.values()) {
                conn.close();
            }
            
            if (fileManager != null) {
                fileManager.close();
            }
            
            if (logger != null) {
                logger.close();
            }
            
            executorService.shutdown();
            scheduler.shutdown();
            
            System.out.println("Peer " + peerId + " shutdown complete.");
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java peerProcess <peerId>");
            System.exit(1);
        }
        
        try {
            int peerId = Integer.parseInt(args[0]);
            peerProcess peer = new peerProcess(peerId);
            peer.start();
            
            // Keep main thread alive
            while (peer.running) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

