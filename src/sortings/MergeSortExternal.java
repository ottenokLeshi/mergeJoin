package sortings;

import entities.LineObject;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class MergeSortExternal {
    private static int fileCounter = 0;
    public static int newLineByteAmount;
    public static int byteSize;

    public static Path sort(String inputFile, String outputFile, int mBperSplit) throws IOException {
        byteSize = 24;
        newLineByteAmount = 1;
        List<Path> pathFiles = splitFile(inputFile, mBperSplit);
        merge(outputFile, pathFiles);
        return Paths.get(outputFile + ".csv");
    }



    /**
     * Split a file into multiples files.
     *
     * @param fileName   Name of file to be split.
     * @param mBperSplit maximum number of MB per file.
     * @throws IOException
     */
    private static List<Path> splitFile(String fileName, int mBperSplit) throws IOException {
        List<Path> partFiles = new ArrayList<>();
        ArrayList<LineObject> arrayList;
        long sourceSize = Files.size(Paths.get(fileName));
        long bytesPerSplit = 1024L * 1024L * mBperSplit;
        long numSplits = sourceSize / bytesPerSplit;
        long remainingBytes = sourceSize % bytesPerSplit;
        byte[] buffer = new byte[byteSize];
        int position = 0;

        try (RandomAccessFile sourceFile = new RandomAccessFile(fileName, "r");
             FileChannel sourceChannel = sourceFile.getChannel()) {
            long internalPos = 0;
            for (; position < numSplits; position++) {
                arrayList = new ArrayList<>();
                for (;internalPos < (position + newLineByteAmount) * bytesPerSplit;
                     internalPos += (byteSize + newLineByteAmount)) {

                    sourceChannel.position(internalPos);
                    sourceFile.read(buffer);
                    arrayList.add(LineObject.getLineObjectFromBuffer(buffer));
                }
                Collections.sort(arrayList);
                writePartToFile(arrayList, partFiles);

            }

            if (remainingBytes > 0) {
                arrayList = new ArrayList<>();
                for (;
                     internalPos < sourceFile.length();
                     internalPos += (byteSize + newLineByteAmount)) {

                    sourceChannel.position(internalPos);
                    sourceFile.read(buffer);
                    arrayList.add(LineObject.getLineObjectFromBuffer(buffer));
                }
                Collections.sort(arrayList);
                writePartToFile(arrayList, partFiles);
            }
        }

        return partFiles;
    }

    private static void writePartToFile(List<LineObject> arrayList, List<Path> partFiles) throws IOException {
        Path fileName = Paths.get((fileCounter++) + ".csv");
        try (BufferedOutputStream bufferedOutputStream =
                     new BufferedOutputStream(new FileOutputStream(fileName.toFile()))) {
            arrayList.forEach(number -> {
                try {
                    bufferedOutputStream.write((number.toString() + "\n").getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        partFiles.add(fileName);
    }

    private static void merge(String fileName, List<Path> partFiles) throws IOException {
        PriorityQueue<HeapNode> heap = new PriorityQueue<>();
        Path outputPath = Paths.get(fileName + ".csv");
        byte[] buffer = new byte[byteSize];

        for (int i = 0; i < partFiles.size(); i++) {
            try (RandomAccessFile sourceFile = new RandomAccessFile(partFiles.get(i).toFile(), "r");
                 FileChannel sourceChannel = sourceFile.getChannel()) {
                sourceChannel.position(0);
                sourceFile.read(buffer);
                heap.add(new HeapNode(LineObject.getLineObjectFromBuffer(buffer), partFiles.get(i), byteSize + newLineByteAmount));
            }
        }

        int count = 0;

        try (BufferedOutputStream bufferedOutputStream =
                     new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))) {
            while (count < partFiles.size()) {
                HeapNode heapNode = heap.poll();
                if (heapNode == null) {
                    break;
                }
                bufferedOutputStream.write((heapNode.lineObject.toString() + "\n").getBytes());

                if (heapNode.lastPosition >= heapNode.fileName.toFile().length()) {
                    count++;
                    continue;
                }

                try (RandomAccessFile sourceFile = new RandomAccessFile(heapNode.fileName.toFile(), "r");
                     FileChannel sourceChannel = sourceFile.getChannel()) {
                    sourceChannel.position(heapNode.lastPosition);
                    sourceFile.read(buffer);
                    heap.add(new HeapNode(LineObject.getLineObjectFromBuffer(buffer), heapNode.fileName,
                            heapNode.lastPosition + byteSize + newLineByteAmount));
                }
            }
        }
    }

    private static class HeapNode implements Comparable<HeapNode>{
        LineObject lineObject;
        Path fileName;
        int lastPosition;

        public HeapNode(LineObject lineObject, Path fileName, int lastPosition) {
            this.lineObject= lineObject;
            this.fileName = fileName;
            this.lastPosition = lastPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HeapNode heapNode = (HeapNode) o;
            return Objects.equals(lineObject, heapNode.lineObject) &&
                    Objects.equals(fileName, heapNode.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lineObject, fileName);
        }

        @Override
        public int compareTo(HeapNode o) {
            return this.lineObject.compareTo(o.lineObject);
        }
    }
}
