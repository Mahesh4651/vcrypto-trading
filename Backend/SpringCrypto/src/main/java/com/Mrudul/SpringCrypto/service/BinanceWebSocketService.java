package com.Mrudul.SpringCrypto.service;

import com.binance.connector.client.impl.WebSocketStreamClientImpl;
import com.binance.connector.client.utils.websocketcallback.WebSocketMessageCallback;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class BinanceWebSocketService {

    private final SimpMessagingTemplate messagingTemplate;
    private final WebSocketStreamClientImpl client;
    private Set<String> availableSymbols = new HashSet<>();
    // Hardcoded list of top 30 coins by market cap (approximate, as of 2025)
    private final Set<String> topCoinSymbols = new HashSet<>(Arrays.asList(
            "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "AVAX", "DOGE", "TRX", "SHIB",
            "LINK", "DOT", "BCH", "NEAR", "LTC", "MATIC", "UNI", "ICP", "FET", "XLM",
            "HBAR", "VET", "CRO", "ATOM", "AAVE", "INJ", "MKR", "GRT", "RUNE", "ALGO"
    ));
    // Cache for latest prices
    private final Map<String, BigDecimal> latestPrices = new ConcurrentHashMap<>();

    public BinanceWebSocketService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
        this.client = new WebSocketStreamClientImpl();
        fetchAllSymbols();
        connectToBinance();
    }

    private void fetchAllSymbols() {
        try {
            String url = "https://api.binance.com/api/v3/exchangeInfo";
            RestTemplate restTemplate = new RestTemplate();
            JSONObject response = new JSONObject(restTemplate.getForObject(url, String.class));

            JSONArray symbolsArray = response.getJSONArray("symbols");
            for (int i = 0; i < symbolsArray.length(); i++) {
                JSONObject symbolObject = symbolsArray.getJSONObject(i);
                String symbol = symbolObject.getString("symbol").toLowerCase();
                if (symbol.endsWith("usdt") || symbol.endsWith("usd")) {
                    availableSymbols.add(symbol);
                }
            }
            System.out.println("✅ Fetched " + availableSymbols.size() + " symbols from Binance");
        } catch (Exception e) {
            System.err.println("❌ Error fetching trading pairs: " + e.getMessage());
        }
    }

    private void connectToBinance() {
        WebSocketMessageCallback callback = message -> {
            try {
                JSONArray tickers = new JSONArray(message);
                for (int i = 0; i < tickers.length(); i++) {
                    JSONObject ticker = tickers.getJSONObject(i);
                    String receivedSymbol = ticker.getString("s").toLowerCase();

                    if (availableSymbols.contains(receivedSymbol)) {
                        String priceStr = ticker.getString("c");
                        BigDecimal price = new BigDecimal(priceStr);
                        String coinSymbol = receivedSymbol.replace("usdt", "").replace("usd", "").toUpperCase();
                        if (coinSymbol.equals("")) {
                            coinSymbol = "USDT";
                        }

                        // Only process updates for top 30 coins
                        if (topCoinSymbols.contains(coinSymbol)) {
                            // Update price cache
                            latestPrices.put(coinSymbol, price);

                            String priceChange24h = ticker.has("P") ? ticker.getString("P") : "0.00";
                            String quoteVolume = ticker.getString("q");
                            String baseVolume = ticker.getString("v");
                            String coinOpen = ticker.getString("o");
                            String coinClose = ticker.getString("c");
                            String coinHigh = ticker.getString("h");
                            String coinLow = ticker.getString("l");
                            String imageUrl = CoinImageMapper.getImageUrl(coinSymbol);

                            Map<String, Object> finalData = new HashMap<>();
                            finalData.put("symbol", coinSymbol);
                            finalData.put("price", priceStr);
                            finalData.put("image", imageUrl);
                            finalData.put("priceChange24h", priceChange24h);
                            finalData.put("quoteVolume", quoteVolume);
                            finalData.put("baseVolume", baseVolume);
                            finalData.put("coinOpen", coinOpen);
                            finalData.put("coinClose", coinClose);
                            finalData.put("coinHigh", coinHigh);
                            finalData.put("coinLow", coinLow);

                            messagingTemplate.convertAndSend("/topic/cryptoPrices", finalData);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        client.allMiniTickerStream(callback);
        System.out.println("✅ Subscribed to Binance tickers for top 30 coins");
    }

    /**
     * Fetch the latest price for a given symbol
     */
    public BigDecimal fetchLivePrice(String symbol) {
        BigDecimal price = latestPrices.get(symbol.toUpperCase());
        if (price == null) {
            throw new IllegalArgumentException("Live price not available for symbol: " + symbol);
        }
        return price;
    }
}