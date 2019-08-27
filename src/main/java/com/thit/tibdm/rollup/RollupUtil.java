package com.thit.tibdm.rollup;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.thit.tibdm.HttpUtil;
import com.thit.tibdm.Main;
import com.thit.tibdm.parse.WarnVariable;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.builder.grouper.TagGrouper;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.RollupResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author: dongzhiquan  Date: 2018/12/24 Time: 16:40
 */
public class RollupUtil {

    private static final Logger LOGGER= LoggerFactory.getLogger(RollupUtil.class);

    private static HttpClient client;

    static {

        client = HttpUtil.getInstance().getClient();
    }



    public static List<String> getCount(String metric,String machine_id,long time,long duration)  {

        List<String> list=new ArrayList<>();
        QueryBuilder queryBuilder=QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(time-duration))
                .setEnd(new Date(time))
                .addMetric(metric)
                .addTag("machine_id",machine_id).setOrder(QueryMetric.Order.DESCENDING);

        QueryResponse query = null;
        try {
            query = client.query(queryBuilder);
        } catch (IOException e) {
            LOGGER.error("{}",e);
        }
        List<DataPoint> dataPoints = null;
        try {
            dataPoints = query.getQueries().get(0).getResults().get(0).getDataPoints();
        } catch (IOException e) {
            LOGGER.error("{}",e);
        }

        String value="";
        for (DataPoint dataPoint : dataPoints) {
             value = dataPoint.getValue().toString();
            list.add(value);
        }
        return list;
    }


    public static String getCountstr(String metric,String machine_id,long time,long duration)  {

        QueryBuilder queryBuilder=QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(time-duration))
                .setEnd(new Date(time))
                .addMetric(metric)
                .addTag("machine_id",machine_id).setOrder(QueryMetric.Order.DESCENDING);

        QueryResponse query = null;
        try {
            query = client.query(queryBuilder);
        } catch (IOException e) {
            LOGGER.error("{}",e);
        }
        List<DataPoint> dataPoints = null;
        try {
            dataPoints = query.getQueries().get(0).getResults().get(0).getDataPoints();
        } catch (IOException e) {
            LOGGER.error("{}",e);
        }

        String value="";
        for (DataPoint dataPoint : dataPoints) {
            value = dataPoint.getValue().toString();
        }
        return value;
    }

    /**
     * 根据协议对聚合任务进行更新
     * 对以YC开头的变量进行更新   如果不用就进行删除
     */

    public static void delete(String id){
        try {
            client.deleteRollup(id);
        } catch (IOException e) {
            LOGGER.error("{}",e);
        }
    }




    /**
     * 根据协议来创建任务
     */
    public static void updateTasks(List<WarnVariable> warnList){

        ImmutableList<RollupTask> tasks = HttpUtil.getrollupTasks();
        List<String> list=new ArrayList<>();
        for (WarnVariable warn : warnList) {
            if (warn.getFunc()!=null){
                list.add(warn.getSerialNumber());
            }
        }
        List<String> list1=new ArrayList<>();
        for (RollupTask task : tasks) {
            list1.add(task.getName());
        }

        for (WarnVariable warn : warnList) {
            List<String> funcList = warn.getFunc();
            List<String> paramList = warn.getParam();
            if (funcList !=null&& funcList.size()>0){
                for (String param : paramList) {
                    if (!list1.contains(param+"_rolluptask")){
                        try {
                            LOGGER.info("createtask");
                            createTask(param,warn.getDuration());
                        }catch (Exception e){
                            LOGGER.error("{}",e);
                        }
                    }
                }
            }
        }

        for (RollupTask task : tasks) {
            String name = task.getName();
            if (name.contains("CY")){
                String[] split = name.split("_");
                if (list.contains(split[0])){
                    delete(task.getId());
                }
            }
        }
    }

    public static void createTask(String metric,double time){
        int time1=(int)time;
        RollupBuilder builder = RollupBuilder.getInstance(metric+"_rolluptask", new RelativeTime(time1, TimeUnit.SECONDS));
        Rollup rollup = builder.addRollup(metric+"_rollup");
        QueryBuilder builder1 = rollup.addQuery();
        builder1.setStart(10,TimeUnit.SECONDS);
        builder1.addMetric(metric).addGrouper(new TagGrouper("machine_id"))
                .addAggregator(AggregatorFactory.createCountAggregator(time1,TimeUnit.MILLISECONDS));
        try {
            RollupResponse rollup1 = client.createRollup(builder);
        } catch (IOException e) {
            LOGGER.error("发生错误{}",e);
        }
    }

}
