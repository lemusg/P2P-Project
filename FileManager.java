import java.io.*;
import java.util.BitSet;

/**
 * Manages file pieces - reading, writing, and tracking which pieces are available
 */
public class FileManager {
    private String filePath;
    private int pieceSize;
    private int numberOfPieces;
    private long fileSize;
    private BitSet bitfield;
    private RandomAccessFile file;

    public FileManager(String peerDirectory, String fileName, int pieceSize, long fileSize, boolean hasFile) throws IOException {
        this.pieceSize = pieceSize;
        this.fileSize = fileSize;
        this.numberOfPieces = (int) Math.ceil((double) fileSize / pieceSize);
        this.bitfield = new BitSet(numberOfPieces);
        
        // Create peer directory if it doesn't exist
        File dir = new File(peerDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        this.filePath = peerDirectory + File.separator + fileName;
        File file = new File(filePath);
        
        if (hasFile) {
            // If peer has the file, set all bits to 1
            for (int i = 0; i < numberOfPieces; i++) {
                bitfield.set(i);
            }
            // Verify file exists
            if (!file.exists()) {
                throw new FileNotFoundException("File should exist: " + filePath);
            }
        } else {
            // Create empty file if it doesn't exist
            if (!file.exists()) {
                file.createNewFile();
                // Pre-allocate file size
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.setLength(fileSize);
                raf.close();
            }
        }
        
        this.file = new RandomAccessFile(filePath, "rw");
    }

    /**
     * Check if a piece is available
     */
    public boolean hasPiece(int pieceIndex) {
        if (pieceIndex < 0 || pieceIndex >= numberOfPieces) {
            return false;
        }
        return bitfield.get(pieceIndex);
    }

    /**
     * Read a piece from the file
     */
    public byte[] readPiece(int pieceIndex) throws IOException {
        if (!hasPiece(pieceIndex)) {
            throw new IOException("Piece " + pieceIndex + " not available");
        }

        int actualPieceSize = getActualPieceSize(pieceIndex);
        byte[] pieceData = new byte[actualPieceSize];
        
        long offset = (long) pieceIndex * pieceSize;
        file.seek(offset);
        file.readFully(pieceData);
        
        return pieceData;
    }

    /**
     * Write a piece to the file
     */
    public void writePiece(int pieceIndex, byte[] pieceData) throws IOException {
        if (pieceIndex < 0 || pieceIndex >= numberOfPieces) {
            throw new IOException("Invalid piece index: " + pieceIndex);
        }

        long offset = (long) pieceIndex * pieceSize;
        file.seek(offset);
        file.write(pieceData);
        file.getFD().sync(); // Force write to disk
        
        bitfield.set(pieceIndex);
    }

    /**
     * Get the actual size of a piece (last piece may be smaller)
     */
    private int getActualPieceSize(int pieceIndex) {
        if (pieceIndex == numberOfPieces - 1) {
            // Last piece
            long remainder = fileSize % pieceSize;
            return remainder == 0 ? pieceSize : (int) remainder;
        }
        return pieceSize;
    }

    /**
     * Get bitfield
     */
    public BitSet getBitfield() {
        return (BitSet) bitfield.clone();
    }

    /**
     * Check if file is complete
     */
    public boolean isFileComplete() {
        for (int i = 0; i < numberOfPieces; i++) {
            if (!bitfield.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get number of pieces currently available
     */
    public int getNumberOfPieces() {
        return bitfield.cardinality();
    }

    /**
     * Get total number of pieces
     */
    public int getTotalNumberOfPieces() {
        return numberOfPieces;
    }

    /**
     * Close the file
     */
    public void close() throws IOException {
        if (file != null) {
            file.close();
        }
    }
}

