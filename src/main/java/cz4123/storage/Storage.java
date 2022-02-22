package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public interface Storage {
    Iterable<List<Integer>> getPositionsIterable();

    List<String> getStationColByPos(List<Integer> positions);

    List<LocalDateTime> getTimestampColByPos(List<Integer> positions);

    List<BigDecimal> getTemperatureColByPos(List<Integer> positions);

    List<BigDecimal> getHumidityColByPos(List<Integer> positions);

    void writeToCSV(List<Result> results, String outputFile);
}
