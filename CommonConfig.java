import java.io.*;
import java.util.*;

/**
 * Reads and stores common configuration from Common.cfg
 */
public class CommonConfig {
    private int numberOfPreferredNeighbors;
    private int unchokingInterval;
    private int optimisticUnchokingInterval;
    private String fileName;
    private long fileSize;
    private int pieceSize;
    private int numberOfPieces;

    public CommonConfig(String configPath) throws IOException {
        readConfig(configPath);
        calculateNumberOfPieces();
    }

    private void readConfig(String configPath) throws IOException {
        File file = new File(configPath);
        if (!file.exists()) {
            throw new FileNotFoundException("Common.cfg not found at: " + configPath);
        }

        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+");
            if (parts.length < 2) continue;

            String key = parts[0];
            String value = parts[1];

            switch (key) {
                case "NumberOfPreferredNeighbors":
                    numberOfPreferredNeighbors = Integer.parseInt(value);
                    break;
                case "UnchokingInterval":
                    unchokingInterval = Integer.parseInt(value);
                    break;
                case "OptimisticUnchokingInterval":
                    optimisticUnchokingInterval = Integer.parseInt(value);
                    break;
                case "FileName":
                    fileName = value;
                    break;
                case "FileSize":
                    fileSize = Long.parseLong(value);
                    break;
                case "PieceSize":
                    pieceSize = Integer.parseInt(value);
                    break;
            }
        }
        scanner.close();
    }

    private void calculateNumberOfPieces() {
        numberOfPieces = (int) Math.ceil((double) fileSize / pieceSize);
    }

    // Getters
    public int getNumberOfPreferredNeighbors() { return numberOfPreferredNeighbors; }
    public int getUnchokingInterval() { return unchokingInterval; }
    public int getOptimisticUnchokingInterval() { return optimisticUnchokingInterval; }
    public String getFileName() { return fileName; }
    public long getFileSize() { return fileSize; }
    public int getPieceSize() { return pieceSize; }
    public int getNumberOfPieces() { return numberOfPieces; }
}

