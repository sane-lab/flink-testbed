package stockv2;

public class Quote implements Comparable<Quote> {
    public String stockId;
    public String acctId;
    public double price;
    public double volume;
    public String buySellIndicator;
    public long timestamp;
    public String payload;

    public Quote(String stockId, String acctId, double price, double volume, String buySellIndicator, long timestamp, String payload) {
        this.stockId = stockId;
        this.acctId = acctId;
        this.price = price;
        this.volume = volume;
        this.buySellIndicator = buySellIndicator;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Quote{" +
                "stockId='" + stockId + '\'' +
                ", acctid='" + acctId +
                ", price=" + price +
                ", volume=" + volume +
                ", buySellIndicator='" + buySellIndicator + '\'' +
                ", timestamp=" + timestamp +
                ", payload=" + payload +
                '}';
    }

    @Override
    public int compareTo(Quote other) {
        return Double.compare(this.price, other.price);
    }
}