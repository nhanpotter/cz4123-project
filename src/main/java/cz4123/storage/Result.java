package cz4123.storage;

import java.math.BigDecimal;
import java.time.LocalDate;

public class Result {
    public LocalDate date;
    public String station;
    public String category;
    public BigDecimal value;

    public Result(LocalDate date, String station, String category, BigDecimal value) {
        this.date = date;
        this.station = station;
        this.category = category;
        this.value = value;
    }
}
