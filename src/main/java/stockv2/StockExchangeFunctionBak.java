package stockv2;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;

import static common.Util.delay;

public class StockExchangeFunctionBak extends KeyedProcessFunction<String, Quote, String> {

    private final int perKeyStateSize;
//    private transient MapState<String, Map<String, NavigableMap<Double, PriorityQueue<Quote>>>> ordersState;
    private transient MapState<String, ArrayList<Quote>> ordersState;

    public StockExchangeFunctionBak(int perKeyStateSize) {
        this.perKeyStateSize = perKeyStateSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, ArrayList<Quote>> descriptor =
                new MapStateDescriptor<>(
                        "orders",
                        TypeInformation.of(String.class),
                        TypeInformation.of(new TypeHint<ArrayList<Quote>>() {}));
        ordersState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Quote quote, Context ctx, Collector<String> out) throws Exception {
        // Add payload to control the state size.
        quote.payload = StringUtils.repeat("A", perKeyStateSize);
        // continue stock exchange
        ArrayList<Quote> stockOrders = ordersState.get(quote.acctId);
        if (stockOrders == null) {
            stockOrders = new ArrayList<>();
//            stockOrders.put("buy", new TreeMap<>(Collections.reverseOrder())); // max-heap for buy orders
//            stockOrders.put("sell", new TreeMap<>()); // min-heap for sell orders
            stockOrders.add(quote);
        }
//        else {
//            if (stockOrders.size() < 5) {
//                stockOrders.add(quote);
//            }
//        }

//        if (quote.buySellIndicator.equals("B")) {
//            stockOrders.get("buy").computeIfAbsent(quote.price, k -> new PriorityQueue<>()).add(quote);
//        } else {
//            stockOrders.get("sell").computeIfAbsent(quote.price, k -> new PriorityQueue<>()).add(quote);
//        }

//        matchOrders(quote.stockId, stockOrders, out);
        delay(100_000);


        ordersState.put(quote.acctId, stockOrders);
    }

    private void matchOrders(String stockId, Map<String, NavigableMap<Double, PriorityQueue<Quote>>> stockOrders, Collector<String> out) {
        NavigableMap<Double, PriorityQueue<Quote>> buyOrders = stockOrders.get("buy");
        NavigableMap<Double, PriorityQueue<Quote>> sellOrders = stockOrders.get("sell");

        while (!buyOrders.isEmpty() && !sellOrders.isEmpty()) {
            double highestBuyPrice = buyOrders.firstKey();
            double lowestSellPrice = sellOrders.firstKey();

            if (highestBuyPrice >= lowestSellPrice) {
                PriorityQueue<Quote> buyQueue = buyOrders.get(highestBuyPrice);
                PriorityQueue<Quote> sellQueue = sellOrders.get(lowestSellPrice);

                Quote buy = buyQueue.peek();
                Quote sell = sellQueue.peek();

                double tradeVolume = Math.min(buy.volume, sell.volume);
                double tradePrice = (buy.price + sell.price) / 2;

                out.collect("Trade executed: Stock ID " + stockId + ", Volume " + tradeVolume + ", Price " + tradePrice);

                buy.volume -= tradeVolume;
                sell.volume -= tradeVolume;

                if (buy.volume == 0) {
                    buyQueue.poll();
                    if (buyQueue.isEmpty()) {
                        buyOrders.remove(highestBuyPrice);
                    }
                }
                if (sell.volume == 0) {
                    sellQueue.poll();
                    if (sellQueue.isEmpty()) {
                        sellOrders.remove(lowestSellPrice);
                    }
                }
            } else {
                break;
            }
        }
    }
}
