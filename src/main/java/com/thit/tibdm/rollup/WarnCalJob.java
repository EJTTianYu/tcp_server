package com.thit.tibdm.rollup;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thit.tibdm.*;
import com.thit.tibdm.fault.FaultEntity;
import com.thit.tibdm.parse.WarnVariable;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: dongzhiquan  Date: 2019/1/11 Time: 14:00
 */
public class WarnCalJob implements Runnable {

    private Map<String, FaultEntity> faultCache;

    private static Map<String, String> currgzMap=Maps.newConcurrentMap() ;

    private final static Logger LOGGER = LoggerFactory.getLogger(WarnCalJob.class);

    private String proId;

    private WarnVariable warn;

    public WarnCalJob(String proId, WarnVariable warn) {
        this.proId = proId;
        this.warn = warn;
        faultCache = Maps.newConcurrentMap();
    }

    public static Map<String, String> getCurrgzMap() {
        return currgzMap;
    }

    public void setCurrgzMap(Map<String, String> currgzMap) {
        this.currgzMap = currgzMap;
    }

    @Override
    public void run() {

        RMap<Object, Object> map = RedisUtil.I.getRedisson().getMap("warnstop_" + proId);
        if (map.containsKey(warn.getSerialNumber())) {
            String stop = map.get(warn.getSerialNumber()).toString();
            if (stop.equals("stop")) {
                Map<String, String> map1 = new HashMap<>();
                map1.put(warn.getSerialNumber(), "");
                RedisUtil.I.getRedisson().getMap("warnstop_" + proId).putAll(map1);
                LOGGER.info("定时任务关闭!");
                Thread.currentThread().stop();
            }
        }

        job();

    }


    private void job() {
//        LOGGER.info("异常聚合运算任务开始执行");
        RBucket<Object> allchs = RedisUtil.I.getRedisson().getBucket("ALLCHS");
        List<String> chs = (List<String>) JSON.parse(allchs.get().toString());

        for (String ch : chs) {
//            if (ch.equals("1701")) {
//                System.out.println("====" + 1701);
//            }
            List<String> funcList = warn.getFunc();
            List<String> paramList = warn.getParam();
            if (funcList != null && funcList.size() != 0) {
                JexlContext mapjexl = new MapContext();
                Map<String, String> parse = new HashMap<>();
                String func = funcList.get(0);
                String cal = "";
                RMap<Object, Object> map = RedisUtil.I.getRedisson().getMap(Contents.WARNCAL_ + ch);
                if (func.equals("count")) {
                    for (String param : paramList) {
                        List<String> countList = RollupUtil.getCount(param, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
//                            List<String> countList = RollupUtil.getCount(param, ch, System.currentTimeMillis(), 1000*60);

                        Double sum = 0.0;
                        for (String count : countList) {
                            parse = (Map<String, String>) JSON.parse(count);
                            sum += Double.parseDouble(parse.get(Contents.BOM_HANDLE));
                        }
                        if (param.contains("-")) {
                            param = param.replace("-", "_");
                        }
                        mapjexl.set(param, sum);
                    }
                    if (parse == null || parse.size() == 0) {
                        continue;
                    }

                } else {
                    if (paramList.size() != 0) {
                        cal = calFun(func, paramList, ch, warn);
                        currgzMap.put(warn.getSerialNumber(), cal);
                        if (cal.equals("true")){
                            cal="1";
                        }else if (cal.equals("false")){
                            cal="0";
                        }
                        map.put(warn.getSerialNumber(), cal);
                        continue;
                    }


                }

                long time = System.currentTimeMillis();
                String jexl = warn.getJexl();
                if (jexl.contains("-")) {
                    jexl = jexl.replace("-", "_");
                }
                cal = JexlPool.I.doConvert(jexl, mapjexl);
                String status = "";
                if (cal.equals("true")) {
                    status = "1";
                } else if (cal.equals("false")) {
                    status = "0";
                }


                if (!map.containsKey(warn.getSerialNumber())) {
                    map.put(warn.getSerialNumber(), "0");
                }

                long collect_time = 0l;
                if (map.containsKey("COLLECT_TIME")) {
                    collect_time = Long.parseLong(map.get("COLLECT_TIME").toString());
                }

                if (!status.equals(map.get(warn.getSerialNumber()))) {

                    if (time >= collect_time + warn.getDuration()) {
                        if (parse == null || parse.size() == 0) {
                            LOGGER.info(ch + "  " + System.currentTimeMillis() + " parse=====" + JSON.toJSONString(parse));

                        }
                        map.put(warn.getSerialNumber(), status);
                        parse.put(Contents.BOM_HANDLE, status);
                        parse.put(Contents.BOM_CODE, warn.getSerialNumber());
                        List<Map<String, String>> warnList = Lists.newArrayList();
                        warnList.add(parse);
                        Map<String, Object> result = Maps.newHashMap();
                        result.put("messageType", "TROUBLE");
                        Map<String, Object> content = Maps.newHashMap();
                        content.put("flag", "WARN");
                        content.put("troubleArr", warnList);
                        result.put("content", content);

                        String msg = JSON.toJSONString(result);

                        try {
                            KafkaProducer.I.send(Config.I.getKafkaTopic(), null, msg.getBytes());
                            LOGGER.info("推送异常 {}", msg);
                        } catch (Exception e) {
                            LOGGER.error("推送异常失败 {}", msg);
                            FileHandle.save2File(msg, Config.I.getWarnFilePath() + File.separator + FileHandle.getFilename(System.currentTimeMillis()) + FileHandle.getFix());
                        }

                    }
                }
            }
        }
    }


    public String calFun(String fun, List<String> paramList, String ch, WarnVariable warn) {
        String cal = "";
        switch (fun) {

            case "average":
                cal = getAverage(paramList, ch, warn) + "";
                break;
            case "sum":
                cal = getSum(paramList, ch, warn) + "";
                break;
            case "range":
                cal = getMax(paramList, ch, warn) - getMin(paramList, ch, warn) + "";
                break;
            case "uniformity":
                cal = getequals(paramList, ch, warn) + "";
                break;
        }
        return cal;
    }

    public double getAverage(List<String> paramList, String ch, WarnVariable warn) {
        double sum = 0;
        double re = 0;
        for (String parm : paramList) {
//            List<String> countList = RollupUtil.getCount(parm, ch, 1547787644000l, new Double(warn.getDuration()).longValue());
            List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
            for (String num : countList) {
                double v = Double.parseDouble(num);
                sum += v;
            }
            if (countList.size() > 0) {
                re = sum / countList.size();
            }
        }
        return re;
    }

    public double getSum(List<String> paramList, String ch, WarnVariable warn) {
        double sum = 0;
        for (String parm : paramList) {
//            List<String> countList = RollupUtil.getCount(parm, ch, 1547787644000l, new Double(warn.getDuration()).longValue());
            List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
            for (String num : countList) {
                double v = Double.parseDouble(num);
                sum += v;
            }


        }
        return sum;
    }

    public static double getMax(List<String> paramList, String ch, WarnVariable warn) {
        double re = 0;
        for (String parm : paramList) {
//            List<String> countList = RollupUtil.getCount(parm, ch, 1547787644000l, new Double(warn.getDuration()).longValue());
            List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
            re = Double.parseDouble(countList.get(0));
            for (String num : countList) {
                double v = Double.parseDouble(num);
                if (v >= re) {
                    re = v;
                }
            }
        }
        return re;
    }

    public static double getMin(List<String> paramList, String ch, WarnVariable warn) {
        double re = 0;
        for (String parm : paramList) {
//            List<String> countList = RollupUtil.getCount(parm, ch, 1547787644000l, new Double(warn.getDuration()).longValue());
            List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
            re = Double.parseDouble(countList.get(0));
            for (String num : countList) {
                double v = Double.parseDouble(num);
                if (v <= re) {
                    re = v;
                }
            }
        }
        return re;
    }

    public boolean getequals(List<String> paramList, String ch, WarnVariable warn) {
        boolean flag = false;
        String str = "";
        String parm = paramList.get(0);
//        List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
        List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
        if (countList.size() != 0) {
            str = countList.get(0);
        } else {
            return false;
        }
        for (String num : countList) {
            if (!str.equals(num)) {
                return flag;
            }
        }
        return false;
    }


}
