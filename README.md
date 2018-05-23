# Problem statement

There are two tables that are stored as plain text files in CSV format. Each table has two columns: a random numerical key and a random string value. Both fields have fixed size: 9 digits for a key, 14 characters for a value.

The goal is to develop a Java application that produces the third table that contains a results of inner join between the two input tables. The third table should contain three fields (numerical key, value from the first input table, value from the second input table) in CSV format. The assignment should be done in two steps:

- Develop a simple implementation that joins two input tables in a reasonably efficient way and can store both tables in RAM if needed.
- Develop an advanced implementation that is able to join two large tables assuming that none of the input tables can fit RAM. Advanced implementation should process ~100Mb files in several minutes (Xmx=64M).

```
input_A.csv
000000011,EKH3-5IDH-DNUK
000000002,P985-U0EX-02QH
000000001,4DG1-WUX0-F16B
000000007,42RY-ES7U-GM6Y
000000009,QL9S-E3DJ-35OH
000000015,UJ8E-G1IV-U9PS
000000000,EL5J-X97W-YRS3
000000012,DS9X-1N3P-KIAW


input_B.csv
000000011,NW8U-C0IQ-WWKV
000000015,Q0Z1-4JN9-PS7U
000000004,289H-D3WD-U518
000000012,ZPMK-N3BU-C33D
000000001,RLT0-DFHH-1U5U
000000012,M3LF-XPWB-8NI6
000000014,17CN-JCX5-CRJR
000000014,NRYG-HFFS-FDS8


result.csv
000000011,EKH3-5IDH-DNUK,NW8U-C0IQ-WWKV
000000015,UJ8E-G1IV-U9PS,Q0Z1-4JN9-PS7U
000000012,DS9X-1N3P-KIAW,ZPMK-N3BU-C33D
000000001,4DG1-WUX0-F16B,RLT0-DFHH-1U5U
000000012,DS9X-1N3P-KIAW,M3LF-XPWB-8NI6
```