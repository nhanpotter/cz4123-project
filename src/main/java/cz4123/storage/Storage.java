package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class Storage implements Iterator<List<Integer>> {
    public abstract List<String> getStationColByPos(List<Integer> positions);

    public abstract List<LocalDateTime> getTimestampColByPos(List<Integer> positions);

    public abstract List<BigDecimal> getTemperatureColByPos(List<Integer> positions);

    public abstract List<BigDecimal> getHumidityColByPos(List<Integer> positions);

    public void writeToCSV(List<Result> results, String outputFile) {
        List<String[]> list = new ArrayList<>();
        String[] header = {"Date", "Station", "Category", "Value"};
        list.add(header);

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
