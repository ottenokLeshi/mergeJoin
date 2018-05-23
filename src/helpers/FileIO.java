package helpers;

import entities.LineObject;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileIO {
    /**
     * Read file and convert data to List
     * @param filename
     * @return List<LineObject>
     * @throws IOException
     */
    public static List<LineObject> read(String filename) throws IOException {
        List<LineObject> resultList = new ArrayList<>();
        Path path = FileSystems.getDefault().getPath(filename);
        Files.lines(path, StandardCharsets.UTF_8).forEach(line -> {
            String[] lineValues = line.split(",");
            int key = Integer.parseInt(lineValues[0]);
            String value = lineValues[1];
            LineObject lineObject = new LineObject(key, value);
            int indexOfLineObject = resultList.indexOf(lineObject);

            if (indexOfLineObject == -1) {
                resultList.add(lineObject);
            } else {
                resultList.get(indexOfLineObject).addValue(value);
            }

        });
        return resultList;
    }

    /**
     * Converts List<LineObject> to file
     * @param filename
     * @param lineObjectList
     * @throws IOException
     */
    public static void write(String filename, List<LineObject> lineObjectList) throws IOException {
        Path path = FileSystems.getDefault().getPath(filename);
        OutputStream out = Files.newOutputStream(path);
        for (LineObject lineObject : lineObjectList) {
            for (String string : lineObject.getValuesList()) {
                StringBuilder stringBuilder = new StringBuilder();
                String key = String.format("%09d", lineObject.getKey());
                stringBuilder.append(key).append(",").append(string).append("\n");

                out.write(stringBuilder.toString().getBytes());
            }
        }
    }
}
