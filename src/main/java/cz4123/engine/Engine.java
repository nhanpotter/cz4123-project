package cz4123.engine;

import cz4123.storage.Result;
import cz4123.storage.Storage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class Engine {
    public static final String TEMPERATURE = "temperature";
    public static final String HUMIDITY = "humidity";

    Storage storage;

    // Query params
    Integer year1;
    Integer year2;
    String station;

    // Store results. Pair<Year, Month>
    Map<String, Map<Pair<Integer, Integer>, BigDecimal>> minValueMap;
    Map<String, Map<Pair<Integer, Integer>, BigDecimal>> maxValueMap;
    // Store results date
    Map<String, Map<Pair<Integer, Integer>, Set<LocalDate>>> minDateMap;
    Map<String, Map<Pair<Integer, Integer>, Set<LocalDate>>> maxDateMap;

    public Engine(Storage storage, Integer year1, Integer year2, String station) {
        this.storage = storage;
        this.year1 = year1;
        this.year2 = year2;
        this.station = station;

        // Initialize
        this.minValueMap = new HashMap<>();
        this.minValueMap.put(HUMIDITY, new HashMap<>());
        this.minValueMap.put(TEMPERATURE, new HashMap<>());

        this.maxValueMap = new HashMap<>();
        this.maxValueMap.put(HUMIDITY, new HashMap<>());
        this.maxValueMap.put(TEMPERATURE, new HashMap<>());

        this.minDateMap = new HashMap<>();
        this.minDateMap.put(HUMIDITY, new HashMap<>());
        this.minDateMap.put(TEMPERATURE, new HashMap<>());

        this.maxDateMap = new HashMap<>();
        this.maxDateMap.put(HUMIDITY, new HashMap<>());
        this.maxDateMap.put(TEMPERATURE, new HashMap<>());
    }

    public void query(String outputFile) {
        while (this.storage.hasNext()) {
            System.out.println("Get data");
            var positions = this.storage.next();
            findResult(positions);
        }

        // Write results to file
        List<Result> results = new ArrayList<>();
        for (var categoryMapEntry : this.minDateMap.entrySet()) {
            String minCategory;
            if (Objects.equals(categoryMapEntry.getKey(), HUMIDITY)) {
                minCategory = Category.MIN_HUMIDITY.toString();
            } else {
                minCategory = Category.MIN_TEMPERATURE.toString();
            }
            for (var resultMapEntry : categoryMapEntry.getValue().entrySet()) {
                BigDecimal minValue = this.minValueMap.get(categoryMapEntry.getKey()).get(resultMapEntry.getKey());
                for (var date : resultMapEntry.getValue()) {
                    results.add(new Result(date, this.station, minCategory, minValue));
                }
            }
        }

        for (var categoryMapEntry : this.maxDateMap.entrySet()) {
            String maxCategory;
            if (Objects.equals(categoryMapEntry.getKey(), HUMIDITY)) {
                maxCategory = Category.MAX_HUMIDITY.toString();
            } else {
                maxCategory = Category.MAX_TEMPERATURE.toString();
            }
            for (var resultMapEntry : categoryMapEntry.getValue().entrySet()) {
                BigDecimal maxValue = this.maxValueMap.get(categoryMapEntry.getKey()).get(resultMapEntry.getKey());
                for (var date : resultMapEntry.getValue()) {
                    results.add(new Result(date, this.station, maxCategory, maxValue));
                }
            }
        }
        this.storage.writeToCSV(results, outputFile);
    }

    /**
     * getResult find the monthly max and min value of humidity and temperature
     * for a list of indices
     */
    private void findResult(List<Integer> positions) {
        positions = queryByStation(positions);
        var positionsMap = queryByYears(positions);
        for (var entry : positionsMap.entrySet()) {
            findMinMaxValue(entry.getValue(), HUMIDITY, entry.getKey());
            findMinMaxValue(entry.getValue(), TEMPERATURE, entry.getKey());
        }
    }

    private List<Integer> queryByStation(List<Integer> positions) {
        List<Integer> res = new ArrayList<>();

        var stationCol = this.storage.getStationColByPos(positions);
        for (int i = 0; i < positions.size(); ++i) {
            var station = stationCol.get(i);

            if (station != null && Objects.equals(station, this.station)) {
                res.add(positions.get(i));
            }
        }
        return res;
    }

    private Map<Pair<Integer, Integer>, List<Integer>> queryByYears(List<Integer> positions) {
        Map<Pair<Integer, Integer>, List<Integer>> res = new HashMap<>();

        List<LocalDateTime> timestampCol = this.storage.getTimestampColByPos(positions);
        for (int i = 0; i < positions.size(); ++i) {
            LocalDateTime timestamp = timestampCol.get(i);

            // Check for null value
            if (timestamp == null) {
                continue;
            }

            int year = timestamp.getYear();
            int month = timestamp.getMonthValue();

            if (year != this.year1 && year != this.year2) {
                continue;
            }

            Pair<Integer, Integer> yearMonthPair = new ImmutablePair<>(year, month);
            if (!res.containsKey(yearMonthPair)) {
                res.put(yearMonthPair, new ArrayList<>());
            }
            res.get(yearMonthPair).add(positions.get(i));
        }

        return res;
    }

    /**
     * @param positions
     * @param col       either humidity or temperature
     * @return
     */
    private void findMinMaxValue(List<Integer> positions, String col,
                                 Pair<Integer, Integer> monthYearPair) {
        List<BigDecimal> dataCol;
        if (Objects.equals(col, HUMIDITY)) {
            dataCol = this.storage.getHumidityColByPos(positions);
        } else {
            dataCol = this.storage.getTemperatureColByPos(positions);
        }

        var minResMap = this.minDateMap.get(col);
        var maxResMap = this.maxDateMap.get(col);
        var minValueMap = this.minValueMap.get(col);
        var maxValueMap = this.maxValueMap.get(col);

        // Initialize if not yet
        if (!minResMap.containsKey(monthYearPair)) {
            minResMap.put(monthYearPair, new HashSet<>());
        }
        if (!maxResMap.containsKey(monthYearPair)) {
            maxResMap.put(monthYearPair, new HashSet<>());
        }


        for (int i = 0; i < positions.size(); ++i) {
            var position = positions.get(i);
            var value = dataCol.get(i);

            // Skip empty data
            if (value == null) {
                continue;
            }

            // Query timestamp from timestamp column
            LocalDateTime datetime = this.storage.getTimestampColByPos(new ArrayList<>(List.of(position))).get(0);
            LocalDate date = datetime.toLocalDate();

            // Min value
            var minValue = minValueMap.get(monthYearPair);
            if ((!minValueMap.containsKey(monthYearPair)) || value.compareTo(minValue) < 0) {
                minValueMap.put(monthYearPair, value);
                minResMap.put(monthYearPair, new HashSet<>());
                minResMap.get(monthYearPair).add(date);
            } else if (value.compareTo(minValue) == 0) {
                minResMap.get(monthYearPair).add(date);
            }

            // Max value
            var maxValue = maxValueMap.get(monthYearPair);
            if ((!maxValueMap.containsKey(monthYearPair)) || value.compareTo(maxValue) > 0) {
                maxValueMap.put(monthYearPair, value);
                maxResMap.put(monthYearPair, new HashSet<>());
                maxResMap.get(monthYearPair).add(date);
            } else if (value.compareTo(maxValue) == 0) {
                maxResMap.get(monthYearPair).add(date);
            }
        }
    }
}
