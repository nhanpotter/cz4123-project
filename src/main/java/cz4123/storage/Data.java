package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Data {
    List<Integer> positionCol;
    List<String> stationCol;
    List<LocalDateTime> timestampCol;
    List<BigDecimal> temperatureCol;
    List<BigDecimal> humidityCol;

    public Data() {
        this.positionCol = new ArrayList<>();
        this.stationCol = new ArrayList<>();
        this.timestampCol = new ArrayList<>();
        this.temperatureCol = new ArrayList<>();
        this.humidityCol = new ArrayList<>();
    }
}
