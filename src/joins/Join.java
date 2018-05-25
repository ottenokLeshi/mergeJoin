package joins;

import entities.LineObject;
import sortings.MergeSortExternal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Join {

    private String pathA;
    private String pathB;
    private String pathOutput;
    private int mBperSplit;

    public Join(String pathA, String pathB, String pathOutput, int mBperSplit) {
        this.pathA = pathA;
        this.pathB = pathB;
        this.pathOutput = pathOutput;
        this.mBperSplit = mBperSplit;
    }

    /**
     * Checks if the same object already exists and merge them, otherwise adds it to the list
     * @param arrayList - list with LineObjects
     * @param buffer    - buffer from file
     */
    private void addValueToList(List<LineObject> arrayList, byte[] buffer) {
        LineObject lineObject = LineObject.getLineObject(buffer);
        if (arrayList.size() != 0 && lineObject.compareTo(arrayList.get(arrayList.size() - 1)) == 0) {
            arrayList.get(arrayList.size() - 1).addValue(lineObject.getValuesList().get(0));
        } else {
            arrayList.add(lineObject);
        }
    }

    /**
     * Sort and joins to files
     * @throws IOException
     */
    public void innerJoin() throws IOException {

        Path pathOut = Paths.get(pathOutput);
        Path pathSortA = MergeSortExternal.sort(pathA, "pathSortA", mBperSplit);
        Path pathSortB = MergeSortExternal.sort(pathB, "pathSortB", mBperSplit);

        System.out.println("Joining starts");
        int i = 0;
        int j = 0;
        long bytesPerSplit = 1024L * 1024L * mBperSplit / 2;
        int byteSize = MergeSortExternal.byteSize;
        int newLineByteAmount = MergeSortExternal.newLineByteAmount;
        byte[] buffer = new byte[byteSize];
        long internalPosA = 0;
        long internalPosB = 0;
        List<LineObject> arrayListA = new ArrayList<>();
        List<LineObject> arrayListB = new ArrayList<>();

        /**
         * Fills the list with objects from first file
         */
        try (RandomAccessFile sourceFile = new RandomAccessFile(pathSortA.toFile(), "r");
             FileChannel sourceChannel = sourceFile.getChannel()) {

            for (;internalPosA < sourceFile.length() % bytesPerSplit;
                     internalPosA += (byteSize + newLineByteAmount)) {
                    sourceChannel.position(internalPosA);
                    sourceFile.read(buffer);

                    addValueToList(arrayListA, buffer);
                }
        }

        /**
         * Fills the list with objects from second file
         */
        try (RandomAccessFile sourceFile = new RandomAccessFile(pathSortB.toFile(), "r");
             FileChannel sourceChannel = sourceFile.getChannel()) {
            for (;internalPosB < sourceFile.length() % bytesPerSplit;
                 internalPosB += (byteSize + newLineByteAmount)) {
                sourceChannel.position(internalPosB);
                sourceFile.read(buffer);
                addValueToList(arrayListB, buffer);


            }
        }

        /**
         * Checks if objects from different lists are equal and writes the result of innerJoin to the specified file
         */
        while (i < arrayListA.size() && j < arrayListB.size()) {

            if (arrayListA.get(i).compareTo(arrayListB.get(j)) < 0) {
                i += 1;
            } else if (arrayListA.get(i).compareTo(arrayListB.get(j)) == 0) {
                /**
                 * join all join results to the object from A list
                 */
                arrayListA.get(i).addValues(arrayListB.get(j).getValuesList());

                try (RandomAccessFile sourceFile = new RandomAccessFile(pathOut.toFile(), "rw");
                     FileChannel sourceChannel = sourceFile.getChannel()) {
                    sourceChannel.position(sourceFile.length());
                    sourceFile.write((arrayListA.get(i).toJoinedString()).getBytes());
                }

                j += 1;
            } else if (arrayListA.get(i).compareTo(arrayListB.get(j)) > 0) {
                j += 1;
            }

            /**
             * Fills the list if file is't empty and interator is at the end of the list
             */
            if (i == arrayListA.size() && internalPosA < Files.size(pathSortA)) {
                try (RandomAccessFile sourceFile = new RandomAccessFile(pathSortA.toFile(), "r");
                     FileChannel sourceChannel = sourceFile.getChannel()) {
                    long startPos = internalPosA;
                    arrayListA.clear();
                    for (;internalPosA < startPos + sourceFile.length() % bytesPerSplit;
                         internalPosA += (byteSize + newLineByteAmount)) {
                        sourceChannel.position(internalPosA);
                        sourceFile.read(buffer);
                        addValueToList(arrayListA, buffer);
                    }
                }
                i = 0;
            }

            if (j == arrayListB.size() && internalPosB < Files.size(pathSortB)) {
                try (RandomAccessFile sourceFile = new RandomAccessFile(pathSortB.toFile(), "r");
                FileChannel sourceChannel = sourceFile.getChannel()) {
                    long startPos = internalPosB;
                    arrayListB.clear();
                    for (;internalPosB < startPos + sourceFile.length() % bytesPerSplit;
                         internalPosB += (byteSize + newLineByteAmount)) {
                        sourceChannel.position(internalPosB);
                        sourceFile.read(buffer);
                        addValueToList(arrayListB, buffer);
                    }
                }
                j = 0;
            }

        }

    }
}
