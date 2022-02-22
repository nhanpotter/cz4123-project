package cz4123.storage;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Utils {
    public static final String EMPTY = "M";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static Data parseCSV(String csvFile) {
        Data data = new Data();
        boolean readHeader = true;
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] lineInArray;
            while ((lineInArray = reader.readNext()) != null) {
                // Assume position column don't have empty value
                if (readHeader) {
                    readHeader = false;
                    continue;
                }
                int position = Integer.parseInt(lineInArray[0]);
                data.positionCol.add(position);
                data.timestampCol.add(parseTimestamp(lineInArray[1]));
                data.stationCol.add(lineInArray[2]);
                data.temperatureCol.add(parseBigDecimal(lineInArray[3]));
                data.humidityCol.add(parseBigDecimal(lineInArray[4]));
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
        List<String[]> list = new ArrayList<>();
        String[] header = {"Date", "Station", "Category", "Value"};
        list.add(header);
        list.addAll(records);

        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputFile), CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            writer.writeAll(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
