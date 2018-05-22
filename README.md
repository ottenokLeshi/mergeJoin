# Problem statement

There are two tables that are stored as plain text files in CSV format. Each table has two columns: a random numerical key and a random string value. Both fields have fixed size: 9 digits for a key, 14 characters for a value.

The goal is to develop a Java application that produces the third table that contains a results of inner join between the two input tables. The third table should contain three fields (numerical key, value from the first input table, value from the second input table) in CSV format. The assignment should be done in two steps:

- Develop a simple implementation that joins two input tables in a reasonably efficient way and can store both tables in RAM if needed.
- Develop an advanced implementation that is able to join two large tables assuming that none of the input tables can fit RAM. Advanced implementation should process ~100Mb files in several minutes (Xmx=64M).