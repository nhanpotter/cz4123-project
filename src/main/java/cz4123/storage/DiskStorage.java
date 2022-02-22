package cz4123.storage;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DiskStorage extends Storage {
    public static final int PAGE_NUMBER = 10;
    int cursor;
    Data timestampCache;

    public DiskStorage(String csvFile) {
        this.cursor = -1;
        Utils.preprocessPartitionCSV(csvFile, PAGE_NUMBER);
    }

    public boolean hasNext() {
        if (this.cursor < PAGE_NUMBER - 1) {
            this.cursor++;
            // Reset cache
            this.timestampCache = null;
            return true;
        }
        return false;
    }

    public List<Integer> next() {
        String positionPath = Paths
                .get(Utils.PREPROCESS_DIR, String.valueOf(this.cursor), Utils.POSITION_FILE)
                .toString();
        Data data = Utils.parseCSV(positionPath, false);

        return data.positionCol;
    }

    public List<String> getStationColByPos(List<Integer> positions) {
        Collections.sort(positions);

        String stationPath = Paths
                .get(Utils.PREPROCESS_DIR, String.valueOf(this.cursor), Utils.STATION_FILE)
                .toString();
        Data data = Utils.parseCSV(stationPath, false);
        List<Integer> indices = positions.stream()
                .map(position -> position - data.positionCol.get(0))
                .collect(Collectors.toList());

        return indices.stream().map(index -> data.stationCol.get(index))
                .collect(Collectors.toList());
    }

    public List<LocalDateTime> getTimestampColByPos(List<Integer> positions) {
        Collections.sort(positions);

        if (this.timestampCache == null) {
            String timestampPath = Paths
                    .get(Utils.PREPROCESS_DIR, String.valueOf(this.cursor), Utils.TIMESTAMP_FILE)
                    .toString();
            Data data = Utils.parseCSV(timestampPath, false);
            this.timestampCache = data;
        }
        List<Integer> indices = positions.stream()
                .map(position -> position - this.timestampCache.positionCol.get(0))
                .collect(Collectors.toList());

        return indices.stream().map(index -> this.timestampCache.timestampCol.get(index))
                .collect(Collectors.toList());
    }

    public List<BigDecimal> getTemperatureColByPos(List<Integer> positions) {
        Collections.sort(positions);

        String temperaturePath = Paths
                .get(Utils.PREPROCESS_DIR, String.valueOf(this.cursor), Utils.TEMPERATURE_FILE)
                .toString();
        Data data = Utils.parseCSV(temperaturePath, false);
        List<Integer> indices = positions.stream()
                .map(position -> position - data.positionCol.get(0))
                .collect(Collectors.toList());

        return indices.stream().map(index -> data.temperatureCol.get(index))
                .collect(Collectors.toList());
    }

    public List<BigDecimal> getHumidityColByPos(List<Integer> positions) {
        Collections.sort(positions);

        String humidityPath = Paths
                .get(Utils.PREPROCESS_DIR, String.valueOf(this.cursor), Utils.HUMIDITY_FILE)
                .toString();
        Data data = Utils.parseCSV(humidityPath, false);
        List<Integer> indices = positions.stream()
                .map(position -> position - data.positionCol.get(0))
                .collect(Collectors.toList());

        return indices.stream().map(index -> data.humidityCol.get(index))
                .collect(Collectors.toList());
    }
}
