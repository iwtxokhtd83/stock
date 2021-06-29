package com.mlp.test.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author Bill Tu
 * @Time 2021-06-25
 */
public class StockService {
    private static final String STOCK_URL = "https://jsonmock.hackerrank.com/api/stocks/search";

    static void openAndClosePrices(String firstDate, String lastDate) {
        ExecutorService executorService = null;
        try {
            DateFormat df = new SimpleDateFormat("d-MMMMM-yyyy");
            List<String> dates = getDates(df, firstDate, lastDate);

            int firstYear = Integer.parseInt(firstDate.split("\\-")[2]);
            int lastYear = Integer.parseInt(lastDate.split("\\-")[2]);
            int noOfThreads = lastYear - firstYear + 1;

            executorService = Executors.newFixedThreadPool(noOfThreads);
            CountDownLatch latch = new CountDownLatch(noOfThreads);

            for (int year = firstYear; year <= lastYear; year++) {
                executorService.submit(new StockRequestTask(latch, String.valueOf(year), dates));
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != executorService) {
                executorService.shutdown();
            }
        }

    }

    public static void main(String[] args) {
        openAndClosePrices("1-January-2000", "11-January-2000");
    }

    private static List<String> getDates(DateFormat df, String firstDate, String lastDate) {
        List<String> dates = new ArrayList<>();
        try {
            Date startDate = df.parse(firstDate);
            Date endDate = df.parse(lastDate);

            if (!dates.contains(firstDate)) {
                dates.add(firstDate);
            }
            while (!startDate.after(endDate)) {
                Calendar c = Calendar.getInstance();
                c.setTime(startDate);
                c.add(Calendar.DATE, 1);
                startDate = c.getTime();
                String date = df.format(startDate);
                if (!dates.contains(date)) {
                    dates.add(date);
                }
            }
            if (!dates.contains(lastDate)) {
                dates.add(lastDate);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dates;
    }

    private static StockResponse retrieveStockInfoByYear(String year) {
        StockResponse stockResponse = null;
        URL urlObj;
        BufferedReader br = null;
        InputStream inputStream = null;
        HttpURLConnection urlCon = null;
        StringBuilder content = new StringBuilder();
        try {
            urlObj = new URL(STOCK_URL + "?date=" + year);
            urlCon = (HttpURLConnection) urlObj.openConnection();
            if (HttpURLConnection.HTTP_OK == urlCon.getResponseCode()) {
                inputStream = urlCon.getInputStream();
                br = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while (null != (line = br.readLine())) {
                    content.append(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (Exception e) {

                }

            }
            if (null != br) {
                try {
                    br.close();
                } catch (Exception e) {

                }

            }
            if (null != urlCon) {
                urlCon.disconnect();
            }
        }
        if (content.length() > 1) {
            Gson gson = new GsonBuilder().create();
            stockResponse = gson.fromJson(content.toString(), StockResponse.class);
        }
        return stockResponse;
    }

    static class StockRequestTask implements Runnable {
        private CountDownLatch latch;
        private String year;
        private List<String> dates;

        public StockRequestTask(CountDownLatch latch, String year, List<String> dates) {
            this.latch = latch;
            this.year = year;
            this.dates = dates;
        }

        @Override
        public void run() {
            try {
                StockResponse stockResponse = retrieveStockInfoByYear(year);
                if (null == stockResponse) {
                    return;
                }
                Map<String, DataItem> dataItemMap = new HashMap<>();
                if (stockResponse.getPer_page() > 0) {
                    List<DataItem> items = stockResponse.getData();
                    for (DataItem item : items) {
                        dataItemMap.put(item.getDate(), item);
                    }
                }
                for (String date : dates) {
                    if (dataItemMap.containsKey(date)) {
                        DataItem item = dataItemMap.get(date);
                        StringBuilder result = new StringBuilder(date);
                        result.append(" ").append(item.getOpen()).append(" ").append(item.getClose());
                        System.out.println(result.toString());
                    }
                }
            } finally {
                latch.countDown();
            }


        }
    }

    static class DataItem {
        private String date = null;
        private double open = 0.0;
        private double high = 0.0;
        private double low = 0.0;
        private double close = 0.0;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public double getOpen() {
            return open;
        }

        public void setOpen(double open) {
            this.open = open;
        }

        public double getHigh() {
            return high;
        }

        public void setHigh(double high) {
            this.high = high;
        }

        public double getLow() {
            return low;
        }

        public void setLow(double low) {
            this.low = low;
        }

        public double getClose() {
            return close;
        }

        public void setClose(double close) {
            this.close = close;
        }

        @Override
        public String toString() {
            return "DataItem{" +
                    "date='" + date + '\'' +
                    ", open=" + open +
                    ", high=" + high +
                    ", low=" + low +
                    ", close=" + close +
                    '}';
        }
    }

    static class StockResponse {
        private int page = 0;
        private int per_page = 0;
        private int total = 0;
        private int total_pages = 0;

        private List<DataItem> data;


        public int getPage() {
            return page;
        }

        public int getPer_page() {
            return per_page;
        }

        public int getTotal() {
            return total;
        }

        public int getTotal_pages() {
            return total_pages;
        }

        public List<DataItem> getData() {
            return data;
        }

        public void setData(List<DataItem> data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "StockResponse{" +
                    "page=" + page +
                    ", per_page=" + per_page +
                    ", total=" + total +
                    ", total_pages=" + total_pages +
                    ", data=" + data +
                    '}';
        }
    }

}
