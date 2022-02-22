package cz4123.engine;

public enum Category {
    MAX_HUMIDITY("Max Humidity"),
    MIN_HUMIDITY("Min Humidity"),
    MAX_TEMPERATURE("Max Temperature"),
    MIN_TEMPERATURE("Min Temperature");

    private final String text;

    Category(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
