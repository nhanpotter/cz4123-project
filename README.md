# Project Milestone 1

## Setup

- Install Maven (https://maven.apache.org/install.html)

## Run the program

1. Put data file `SingaporeWeather.csv` at the project root.

2. Compile

```bash
mvn compile
```

3. Execute

```bash
mvn exec:java -Dexec.mainClass=cz4123.App
```

4. The outputs are two CSV file `ScanResult.csv` (from Task 1) and
   `ScanResult2.csv` (from Task 2)