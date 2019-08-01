package com.thit.tibdm.metrics;


import com.alibaba.fastjson.JSON;
import com.codahale.metrics.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2019/3/21.
 */
public class FalconReporter extends ScheduledReporter {
    private static final Logger logger= LoggerFactory.getLogger(FalconReporter.class);
    private String endpoint="test_hostname";
    long step=60;
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }


    private void reportHistogram(long timestamp,String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();
        send(endpoint,"count",timestamp,step,histogram.getCount(),"type=HISTOGRAM,value=count");
        send(endpoint,"min",timestamp,step,snapshot.getMin(),"type=HISTOGRAM,value=min");
        send(endpoint,"max",timestamp,step,snapshot.getMax(),"type=HISTOGRAM,value=max");
        send(endpoint,"mean",timestamp,step,snapshot.getMean(),"type=HISTOGRAM,value=mean");
        send(endpoint,"stddev",timestamp,step,snapshot.getStdDev(),"type=HISTOGRAM,value=stddev");
        send(endpoint,"median",timestamp,step,snapshot.getMedian(),"type=HISTOGRAM,value=median");
        send(endpoint,"p75",timestamp,step,snapshot.get75thPercentile(),"type=HISTOGRAM,value=p75");
        send(endpoint,"p95",timestamp,step,snapshot.get95thPercentile(),"type=HISTOGRAM,value=p95");
        send(endpoint,"p98",timestamp,step,snapshot.get98thPercentile(),"type=HISTOGRAM,value=p98");
        send(endpoint,"p99",timestamp,step,snapshot.get99thPercentile(),"type=HISTOGRAM,value=p99");
        send(endpoint,"p999",timestamp,step,snapshot.get999thPercentile(),"type=HISTOGRAM,value=p999");

    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        send(endpoint,"count",timestamp,step,counter.getCount(),"type=COUNTER,value=count");
    }

    private  void send(String endpoint,String metric,long timestamp,long  step,double value,String tags){
        try {
            HttpClient client = new DefaultHttpClient( );
            HttpPost post = new HttpPost("http://192.168.2.173:1988/v1/push");
            HashMap<String, Object> map = new HashMap<String, Object>();
            map.put("endpoint",endpoint);
            map.put("metric",metric);
            map.put("timestamp",timestamp);
            map.put("step", step);
            map.put("value", value);
            map.put("counterType", "GAUGE");
            map.put("tags",tags);
            StringEntity stringEntity = null;
            logger.info("推送数据 {} ",JSON.toJSONString(map));
            stringEntity = new StringEntity("["+ JSON.toJSONString(map)+"]");
            stringEntity.setContentEncoding("UTF-8");
            stringEntity.setContentType("application/json");
            post.setEntity(stringEntity);

            HttpResponse response = client.execute(post);
//            if(response.getStatusLine().getStatusCode() == 200) {
//                HttpEntity entity = response.getEntity();
//                System.out.println(EntityUtils.toString(entity));
//            }

        }catch (Exception e){
            logger.error("{}",e);
        }
    }



    private void reportGauge(long timestamp, String name, Gauge gauge) {
        send(endpoint,"value",timestamp,step,Long.parseLong(gauge.getValue().toString()),"type=GAUGE,value=value");
    }

    private void reportMeter(long timestamp,String name, Meter meter) {

        send(endpoint,"count",timestamp,step,meter.getCount(),"type=GAUGE,value=count");
        send(endpoint,"mean_rate",timestamp,step,convertRate(meter.getMeanRate()),"type=GAUGE,value=mean_rate");
        send(endpoint,"m1",timestamp,step,convertRate(meter.getOneMinuteRate()),"type=GAUGE,value=m1");
        send(endpoint,"m5",timestamp,step,convertRate(meter.getFiveMinuteRate()),"type=GAUGE,value=m5");
        send(endpoint,"m15",timestamp,step,convertRate(meter.getFifteenMinuteRate()),"type=GAUGE,value=m15");
    }



    private void reportTimer(long timestamp,String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();
        send(endpoint,"count",timestamp,step,timer.getCount(),"type=TIMER,value=count");
        send(endpoint,"min",timestamp,step,convertDuration(snapshot.getMin()),"type=TIMER,value=min");
        send(endpoint,"max",timestamp,step,convertDuration(snapshot.getMax()),"type=TIMER,value=max");
        send(endpoint,"mean",timestamp,step,convertDuration(snapshot.getMean()),"type=TIMER,value=mean");
        send(endpoint,"stddev",timestamp,step,convertDuration(snapshot.getStdDev()),"type=TIMER,value=stddev");
        send(endpoint,"median",timestamp,step,convertDuration(snapshot.getMedian()),"type=TIMER,value=median");
        send(endpoint,"p75",timestamp,step,convertDuration(snapshot.get75thPercentile()),"type=TIMER,value=p75");
        send(endpoint,"p95",timestamp,step,convertDuration(snapshot.get95thPercentile()),"type=TIMER,value=p95");
        send(endpoint,"p98",timestamp,step,convertDuration(snapshot.get98thPercentile()),"type=TIMER,value=p98");
        send(endpoint,"p99",timestamp,step,convertDuration(snapshot.get99thPercentile()),"type=TIMER,value=p99");
        send(endpoint,"p999",timestamp,step,convertDuration(snapshot.get999thPercentile()),"type=TIMER,value=p999");
        send(endpoint,"mean_rate",timestamp,step,convertRate(timer.getMeanRate()),"type=TIMER,value=mean_rate");
        send(endpoint,"m1",timestamp,step,convertRate(timer.getOneMinuteRate()),"type=TIMER,value=m1");
        send(endpoint,"m5",timestamp,step,convertRate(timer.getFiveMinuteRate()),"type=TIMER,value=m5");
        send(endpoint,"m15",timestamp,step,convertRate(timer.getFifteenMinuteRate()),"type=TIMER,value=m15");
    }

    public static class Builder{
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Builder(MetricRegistry registry){
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor=null;
            this.shutdownExecutorOnStop = true;
        }

        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }



        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }


        public FalconReporter build(){
            return new FalconReporter(registry,rateUnit,durationUnit,filter,executor,shutdownExecutorOnStop);
        }


    }

    private FalconReporter(MetricRegistry registry,
                           TimeUnit rateUnit,
                           TimeUnit durationUnit,
                           MetricFilter filter,
                           ScheduledExecutorService executor,
                           boolean shutdownExecutorOnStop){
       super(registry,"falcon-reporter",filter,rateUnit,durationUnit,executor,shutdownExecutorOnStop);
    }
}
