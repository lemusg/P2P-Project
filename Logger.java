import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Logger for peer events
 */
public class Logger {
    private PrintWriter writer;
    private SimpleDateFormat dateFormat;
    private int peerId;

    public Logger(int peerId, String logFilePath) throws IOException {
        this.peerId = peerId;
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        File logFile = new File(logFilePath);
        // Create parent directory if it doesn't exist
        if (logFile.getParentFile() != null) {
            logFile.getParentFile().mkdirs();
        }
        
        this.writer = new PrintWriter(new FileWriter(logFile, false)); // Overwrite mode
    }

    private String getCurrentTime() {
        return dateFormat.format(new Date());
    }

    public void logTcpConnectionMade(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " makes a connection to Peer " + otherPeerId + ".");
        writer.flush();
    }

    public void logTcpConnectionReceived(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " is connected from Peer " + otherPeerId + ".");
        writer.flush();
    }

    public void logPreferredNeighborsChanged(int[] preferredNeighbors) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < preferredNeighbors.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(preferredNeighbors[i]);
        }
        writer.println(getCurrentTime() + ": Peer " + peerId + " has the preferred neighbors [" + sb.toString() + "].");
        writer.flush();
    }

    public void logOptimisticallyUnchokedNeighborChanged(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " has the optimistically unchoked neighbor " + otherPeerId + ".");
        writer.flush();
    }

    public void logUnchoked(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " is unchoked by " + otherPeerId + ".");
        writer.flush();
    }

    public void logChoked(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " is choked by " + otherPeerId + ".");
        writer.flush();
    }

    public void logReceivedHave(int otherPeerId, int pieceIndex) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " received the 'have' message from " + otherPeerId + " for the piece " + pieceIndex + ".");
        writer.flush();
    }

    public void logReceivedInterested(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " received the 'interested' message from " + otherPeerId + ".");
        writer.flush();
    }

    public void logReceivedNotInterested(int otherPeerId) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " received the 'not interested' message from " + otherPeerId + ".");
        writer.flush();
    }

    public void logDownloadedPiece(int pieceIndex, int otherPeerId, int numberOfPieces) {
        writer.println(getCurrentTime() + ": Peer " + peerId + " has downloaded the piece " + pieceIndex + " from " + otherPeerId + ". Now the number of pieces it has is " + numberOfPieces + ".");
        writer.flush();
    }

    public void logDownloadComplete() {
        writer.println(getCurrentTime() + ": Peer " + peerId + " has downloaded the complete file.");
        writer.flush();
    }

    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}

