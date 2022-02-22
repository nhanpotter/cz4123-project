package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class MemoryStorage extends Storage {
    Data data;
    private boolean start;

    public MemoryStorage(String csvFile) {
        this.data = Utils.parseCSV(csvFile, true);
        this.start = true;

        // Make sure id increase monotonically
        int size = this.data.positionCol.size();
        assert this.data.positionCol.get(size - 1) == size - 1;
    }

    public boolean hasNext() {
        if (start) {
            start = false;
            return true;
        }
        return false;
    }

    public List<Integer> next() {
        return this.data.positionCol;
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
}
