import java.io.*;
import java.util.*;

/**
 * Represents information about a single peer
 */
public class PeerInfo {
    private int peerId;
    private String hostName;
    private int listeningPort;
    private boolean hasFile;

    public PeerInfo(int peerId, String hostName, int listeningPort, boolean hasFile) {
        this.peerId = peerId;
        this.hostName = hostName;
        this.listeningPort = listeningPort;
        this.hasFile = hasFile;
    }

    // Getters
    public int getPeerId() { return peerId; }
    public String getHostName() { return hostName; }
    public int getListeningPort() { return listeningPort; }
    public boolean hasFile() { return hasFile; }
}

/**
 * Reads and stores peer information from PeerInfo.cfg
 */
class PeerInfoReader {
    public static List<PeerInfo> readPeerInfo(String configPath) throws IOException {
        List<PeerInfo> peers = new ArrayList<>();
        File file = new File(configPath);
        
        if (!file.exists()) {
            throw new FileNotFoundException("PeerInfo.cfg not found at: " + configPath);
        }

        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+");
            if (parts.length < 4) continue;

            int peerId = Integer.parseInt(parts[0]);
            String hostName = parts[1];
            int listeningPort = Integer.parseInt(parts[2]);
            boolean hasFile = parts[3].equals("1");

            peers.add(new PeerInfo(peerId, hostName, listeningPort, hasFile));
        }
        scanner.close();

        return peers;
    }
}

