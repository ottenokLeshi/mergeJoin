package joins;

import entities.LineObject;
import helpers.FileIO;
import sortings.MergeSortInternal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Join {

    private String pathA;
    private String pathB;
    private String pathOutput;

    private Join() {
    }

    public Join(String pathA, String pathB, String pathOutput) {
        this.pathA = pathA;
        this.pathB = pathB;
        this.pathOutput = pathOutput;
    }

    public void innerJoin() throws IOException {

        List<LineObject> A = FileIO.read(pathA);
        List<LineObject> B = FileIO.read(pathB);

        MergeSortInternal.sort(A, 0, A.size() - 1);
        MergeSortInternal.sort(B, 0, B.size() - 1);

        int i = 0;
        int j = 0;
        List<LineObject> lineObjectList = new ArrayList<>();

        while (i < A.size() && j < B.size()) {
            if (A.get(i).compareTo(B.get(j)) < 0) {
                i += 1;
            }

            if (A.get(i).compareTo(B.get(j)) > 0) {
                j += 1;
            }

            if (A.get(i).compareTo(B.get(j)) == 0) {
                A.get(i).addValues(B.get(j).getValuesList());
                lineObjectList.add(A.get(i));
                i += 1;
                j += 1;
            }
        }

        FileIO.write(pathOutput, lineObjectList);
    }


}
