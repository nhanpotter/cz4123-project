package cz4123;

import cz4123.engine.Engine;
import cz4123.storage.MemoryStorage;
import cz4123.storage.Storage;

import java.io.File;

public class App {
    public static final String CSV_FILE = "SingaporeWeather.csv";
    public static final String OUTPUT_FILE = "ScanResult.csv";
    public static final int YEAR1 = 2011;
    public static final int YEAR2 = 2021;
    public static final String STATION = "Paya Lebar";

    public static void main(String[] args) {
        // Check if file exists
        File f = new File(CSV_FILE);
        if (!f.exists()) {
            System.out.printf("File %s not found", CSV_FILE);
            return;
        }

        task1();
        System.out.println("Done");
    }

    public static void task1() {
        System.out.println("Running Task 1");
        Storage storage = new MemoryStorage(CSV_FILE);
        Engine engine = new Engine(storage, YEAR1, YEAR2, STATION);
        engine.query(OUTPUT_FILE);
        System.out.println("Finish Task 1");
    }
}