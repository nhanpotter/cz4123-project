package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public interface Storage {
    public abstract Iterable<List<Integer>> getPositionsIterable();

    public abstract List<String> getStationColByPos(List<Integer> positions);


    public abstract List<LocalDateTime> getTimestampColByPos(List<Integer> positions);


    public abstract List<BigDecimal> getTemperatureColByPos(List<Integer> positions);

    public abstract List<BigDecimal> getHumidityColByPos(List<Integer> positions);

    public abstract void writeToCSV(List<Result> results, String outputFile);
}
