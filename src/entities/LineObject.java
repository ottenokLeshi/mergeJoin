package entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Object with a unique key and an array of lines from the document
 */
public class LineObject implements Comparable<LineObject>{
    private Integer key;
    private List<String> valuesList;

    public LineObject(int key, String value) {
        this.key = key;
        this.valuesList = new ArrayList<>();
        this.valuesList.add(value);
    }

    public void addValue(String value) {
        valuesList.add(value);
    }

    private void setValuesList(List<String> valuesList) {
        this.valuesList = valuesList;
    }

    /**
     * Method that unites string sets by the Cartesian product
     *
     * @param anotherValuesList - List of Strings that correspond to unique key
     */
    public void addValues(List<String> anotherValuesList) {
        List<String> resultValuesList = new ArrayList<>();
        for (int i = 0; i < valuesList.size(); i++) {
            for (int j = 0; j < anotherValuesList.size(); j++) {
                resultValuesList.add(valuesList.get(i) + "," + anotherValuesList.get(j));
            }
        }
        setValuesList(resultValuesList);
    }

    public int getKey() {
        return key;
    }

    public List<String> getValuesList() {
        return valuesList;
    }

    @Override
    public int compareTo(LineObject o) {
        return key.compareTo(o.getKey());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LineObject that = (LineObject) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, valuesList);
    }
}
