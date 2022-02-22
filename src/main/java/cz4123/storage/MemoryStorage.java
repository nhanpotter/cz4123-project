package cz4123.storage;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MemoryStorage implements Storage {
    Data data;

    public MemoryStorage(String csvFile) {
        this.data = Utils.parseCSV(csvFile);
    }

    /**
     * getPositionsIterable return iterable of 1 whole list of positions
     */
    public Iterable<List<Integer>> getPositionsIterable() {
        List<List<Integer>> iterable = new ArrayList<>();
        iterable.add(this.data.positionCol);
        return iterable;
    }

    public List<String> getStationColByPos(List<Integer> positions) {
        return positions.stream().map(
                index -> this.data.stationCol.get(index)
        ).collect(Collectors.toList());
    }

    public List<LocalDateTime> getTimestampColByPos(List<Integer> positions) {
        return positions.stream().map(
                index -> this.data.timestampCol.get(index)
        ).collect(Collectors.toList());
    }

    public List<BigDecimal> getTemperatureColByPos(List<Integer> positions) {
        return positions.stream().map(
                index -> this.data.temperatureCol.get(index)
        ).collect(Collectors.toList());
    }

    public List<BigDecimal> getHumidityColByPos(List<Integer> positions) {
        return positions.stream().map(
                index -> this.data.humidityCol.get(index)
        ).collect(Collectors.toList());
    }

    public void writeToCSV(List<Result> results, String outputFile) {
        List<String[]> list = new ArrayList<>();

        for (var result : results) {
            String timestampStr = Utils.convertString(result.date);
            String station = result.station;
            String category = result.category;
            String valueStr = result.value.toString();
            String[] record = {timestampStr, station, category, valueStr};
            list.add(record);
        }

        Utils.writeCSV(list, outputFile);
    }
}
