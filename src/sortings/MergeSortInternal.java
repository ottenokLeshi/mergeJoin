package sortings;

import entities.LineObject;

import java.util.ArrayList;
import java.util.List;

public abstract class MergeSortInternal {

    private static void merge(List<LineObject> arrayList, int begin, int middle, int end) {
        int n = middle - begin + 1;
        int m = end - middle;

        ArrayList<LineObject> leftList = new ArrayList();
        ArrayList<LineObject> rightList = new ArrayList();

        for (int i = 0; i < n; i++)
            leftList.add(arrayList.get(begin + i));
        for (int j = 0; j < m; j++)
            rightList.add(arrayList.get(middle + j + 1));

        int i = 0;
        int j = 0;
        int k = begin;

        while (i < n && j < m) {
            if (leftList.get(i).compareTo(rightList.get(j)) < 0) {
                arrayList.set(k, leftList.get(i));
                i += 1;
            } else {
                arrayList.set(k, rightList.get(j));
                j += 1;
            }
            k += 1;
        }

        while (i < n) {
            arrayList.set(k, leftList.get(i));
            i += 1;
            k += 1;
        }

        while (j < m){
            arrayList.set(k, rightList.get(j));
            j += 1;
            k += 1;
        }
    }

    public static void sort(List<LineObject> arrayList, int begin, int end) {
        if (begin < end) {
            int middle = (begin + end) /2;
            sort(arrayList, begin, middle);
            sort(arrayList, middle + 1, end);
            merge(arrayList, begin, middle, end);
        }
    }
}
