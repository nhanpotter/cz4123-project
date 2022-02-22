package cz4123.storage;

import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Utils {
    public static final String EMPTY = "M";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // For preprocessing
    public static final String PREPROCESS_DIR = "preprocess";
    public static final String POSITION_FILE = "position.csv";
    public static final String STATION_FILE = "station.csv";
    public static final String TIMESTAMP_FILE = "timestamp.csv";
    public static final String TEMPERATURE_FILE = "temperature.csv";
    public static final String HUMIDITY_FILE = "humidity.csv";

    public static Data parseCSV(String csvFile, boolean skipHeader) {
        Data data = new Data();
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] lineInArray;
            while ((lineInArray = reader.readNext()) != null) {
                // Assume position column don't have empty value
                if (skipHeader) {
                    skipHeader = false;
                    continue;
                }
                data.positionCol.add(Integer.parseInt(lineInArray[0]));

                if (csvFile.contains(POSITION_FILE)) {
                } else if (csvFile.contains(TIMESTAMP_FILE)) {
                    data.timestampCol.add(parseTimestamp(lineInArray[1]));
                } else if (csvFile.contains(STATION_FILE)) {
                    data.stationCol.add(lineInArray[1]);
                } else if (csvFile.contains(TEMPERATURE_FILE)) {
                    data.temperatureCol.add(parseBigDecimal(lineInArray[1]));
                } else if (csvFile.contains(HUMIDITY_FILE)) {
                    data.humidityCol.add(parseBigDecimal(lineInArray[1]));
                } else {
                    data.timestampCol.add(parseTimestamp(lineInArray[1]));
                    data.stationCol.add(lineInArray[2]);
                    data.temperatureCol.add(parseBigDecimal(lineInArray[3]));
                    data.humidityCol.add(parseBigDecimal(lineInArray[4]));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    public static LocalDateTime parseTimestamp(String string) {
        if (Objects.equals(string, EMPTY)) {
            return null;
        }

        return LocalDateTime.parse(string, DATE_TIME_FORMATTER);
    }

    public static BigDecimal parseBigDecimal(String string) {
        if (Objects.equals(string, EMPTY)) {
            return null;
        }
        return new BigDecimal(string);
    }

    public static String convertString(LocalDate date) {
        return DATE_FORMATTER.format(date);
    }

    public static void writeCSV(List<String[]> records, String outputFile) {
        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputFile), CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            writer.writeAll(records);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void preprocessPartitionCSV(String srcFile, int pageNumber) {
        System.out.println("Preprocessing...");
        try {
            CSVReader reader = new CSVReader(new FileReader(srcFile));
            List<String[]> records = reader.readAll();
            // Remove header
            records.remove(0);

            var recordsPages = Lists
                    .partition(records, (int) Math.ceil((double) records.size() / pageNumber));
            assert recordsPages.size() == pageNumber;

            for (int i = 0; i < recordsPages.size(); ++i) {
                var recordsPage = recordsPages.get(i);
                var preprocessPath = Paths.get(PREPROCESS_DIR, String.valueOf(i));
                Files.createDirectories(preprocessPath);

                // Position
                List<String[]> positionCol = recordsPage.stream().map(record -> new String[]{record[0]})
                        .collect(Collectors.toList());
                var positionFilePath = Paths.get(preprocessPath.toString(), POSITION_FILE);
                writeCSV(positionCol, positionFilePath.toString());

                // Timestamp
                List<String[]> timestampCol = recordsPage.stream()
                        .map(record -> new String[]{record[0], record[1]})
                        .collect(Collectors.toList());
                var timestampFilePath = Paths.get(preprocessPath.toString(), TIMESTAMP_FILE);
                writeCSV(timestampCol, timestampFilePath.toString());

                // Station
                List<String[]> stationCol = recordsPage.stream()
                        .map(record -> new String[]{record[0], record[2]})
                        .collect(Collectors.toList());
                var stationFilePath = Paths.get(preprocessPath.toString(), STATION_FILE);
                writeCSV(stationCol, stationFilePath.toString());

                // Temperature
                List<String[]> temperatureCol = recordsPage.stream()
                        .map(record -> new String[]{record[0], record[3]})
                        .collect(Collectors.toList());
                var temperatureFilePath = Paths.get(preprocessPath.toString(), TEMPERATURE_FILE);
                writeCSV(temperatureCol, temperatureFilePath.toString());

                // Humidity
                List<String[]> humidityCol = recordsPage.stream()
                        .map(record -> new String[]{record[0], record[4]})
                        .collect(Collectors.toList());
                var humidityFilePath = Paths.get(preprocessPath.toString(), HUMIDITY_FILE);
                writeCSV(humidityCol, humidityFilePath.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Finish Preprocessing...");
    }
}
