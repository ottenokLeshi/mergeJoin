import joins.Join;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        Join join = new Join("inputA.csv", "inputB.csv", "output.csv", 10);
        join.innerJoin();
    }

}