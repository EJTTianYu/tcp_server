//package com.thit.tibdm.rollup;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.thit.tibdm.*;
//import com.thit.tibdm.parse.ContentDecode;
//import com.thit.tibdm.parse.ParseRule;
//import com.thit.tibdm.parse.WarnVariable;
//import org.apache.commons.collections4.map.HashedMap;
//import org.apache.commons.jexl2.JexlContext;
//import org.apache.commons.jexl2.MapContext;
//import org.quartz.Job;
//import org.quartz.JobExecutionContext;
//import org.quartz.JobExecutionException;
//import org.redisson.api.RBucket;
//import org.redisson.api.RMap;
//
//import java.io.File;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author: dongzhiquan  Date: 2019/1/10 Time: 15:51
// */
//public class RAMJob implements Job {
//    @Override
//    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
//        System.out.println(new Date());
//
////        RollupUtil.getCount("YC18","1701",System.currentTimeMillis(),10000);
//
////        System.out.println("====");
//
//
//
//
//
//    }
//
//
//    public static void main(String[] args) {
////        String value=RollupUtil.getCount("YC18-1","1710",System.currentTimeMillis(),10000);
////
////        System.out.println("==="+value);
//
//
//
////        docount();
//
//        JexlContext mapjexl = new MapContext();
//        mapjexl.set("YC1_1",3.0);
//        mapjexl.set("YC2_1",3.0);
//        String s = JexlPool.I.doConvert("YC1_1+YC2_1", mapjexl);
//        System.out.println(s);
//
//
//    }
//
//
//    //五秒中执行一次？
//    public static void docount(){
//
//        //计算所有车十秒钟的次数
//        //找出所有车
//        //找出协议里的异常变量
//        //查询十秒钟的数据
//        //发送异常
//
//        Map<String, ParseRule> ruleMap = ContentDecode.I.getRuleMap();
//
//        Map<String, String> machineMapperPro = ContentDecode.I.getMachineMapperPro();
//
//        RBucket<Object> allchs = RedisUtil.I.getRedisson().getBucket("ALLCHS");
//        List<String> chs = (List<String>)JSON.parse(allchs.get().toString());
//
//        for (String ch : chs) {
//            System.out.println(ch);
//            if (machineMapperPro.containsKey(ch)){
//
//                String proId = machineMapperPro.get(ch);
//                if (ch.contains("1704")){
//                    System.out.println(55555);
//                }
//                if (ruleMap.containsKey(proId)) {
//
//                    ParseRule rule = ruleMap.get(proId);
//
//                    List<WarnVariable> warnVaribales = rule.getWarnVaribales();
//
//                    for (WarnVariable warn : warnVaribales) {
//                        List<String> funcList = warn.getFunc();
//                        List<String> paramList = warn.getParam();
//                        if (funcList!=null&&funcList.size()!=0){
//                            JexlContext mapjexl = new MapContext();
//                            Map<String,String> parse=new HashMap<>();
//                            for (String func : funcList) {
//                                if (func.equals("count")){
//                                    for (String param : paramList) {
//                                        List<String> countList =  RollupUtil.getCount(param, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
//                                        Double sum=0.0;
//                                        for (String count : countList) {
//                                            parse =(Map<String,String>) JSON.parse(count);
//                                            sum+=Double.parseDouble(parse.get(Contents.BOM_HANDLE));
//                                        }
//
//                                        mapjexl.set(param, sum);
//                                    }
//
//                                }
//                            }
//
//                            String s = JexlPool.I.doConvert(warn.getJexl(), mapjexl);
//
//
//                            if (s.equals("true")){
//                                parse.put(Contents.BOM_HANDLE,"1");
//                                List<Map<String, String>> warnList = Lists.newArrayList();
//                                warnList.add(parse);
//                                Map<String, Object> result = Maps.newHashMap();
//                                result.put("messageType", "TROUBLE");
//                                Map<String, Object> content = Maps.newHashMap();
//                                content.put("flag", "WARN");
//                                content.put("troubleArr", warnList);
//                                result.put("content", content);
//
//                                String msg = JSON.toJSONString(result);
//
//                                try {
//                                    KafkaProducer.I.send(Config.I.getKafkaTopic(), null, msg.getBytes());
////                                    .info("推送异常 {}", msg);
//                                } catch (Exception e) {
////                                    LOGGER.error("推送异常失败 {}", msg);
//                                    FileHandle.save2File(msg, Config.I.getWarnFilePath() + File.separator + FileHandle.getFilename(System.currentTimeMillis()) + FileHandle.getFix());
//                                }
//
//                            }
//
//
//                        }
//
//                    }
//
//
//                }
//
//            }
//
//
//        }
//
//
//        System.out.println(chs.size());
//
//
//    }
//}
