//package com.thit.tibdm.rollup;
//
//import com.thit.tibdm.parse.WarnVariable;
//import org.apache.commons.jexl2.parser.ParseException;
//import org.quartz.*;
//import org.quartz.impl.StdSchedulerFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Date;
//
///**
// * @author: dongzhiquan  Date: 2019/1/10 Time: 15:30
// */
//public class WarnJob {
//
//    private final static Logger LOGGER= LoggerFactory.getLogger(WarnJob.class);
//
//    public static void main(String[] args) {
//
//        //遍历异常变量 生成定时任务
//        //每个定时任务计算一个变量
//        //将定时任务和变量绑定到一块
//
//
//
////        WarnVariable warn=new WarnVariable();
////        warn.setDuration(70000);
////        job(warn);
//        String cal="";
//        double duration=1000*60*60*50;
//
//        if (duration>0&&duration<60000){
//            cal="0/"+new Double(duration/1000).intValue() +" * * * * ?";
//        }else if (duration>=60000&&duration<60*60000){
//            cal="0 0/"+ new Double(duration/1000/60).intValue()+" * * * ?";
//        }else if (duration>=60*60000&&duration<24*60*60000){
//            cal="0 0 0/"+  new Double(duration/60/60000).intValue()+" * * ?";
//        }else if (duration>=24*60*60000){
//            cal="0 0 0 0/"+  new Double(duration/24/60/60000).intValue()+" * ?";
//        }
//
//        System.out.println(cal);
//    }
//
//    public static void job(WarnVariable warn){
//
//        try {
//
//
//        //1.创建Scheduler的工厂
//        SchedulerFactory sf = new StdSchedulerFactory();
//        //2.从工厂中获取调度器实例
//        Scheduler scheduler = sf.getScheduler();
//
//
//        //3.创建JobDetail
//        JobDetail jb = JobBuilder.newJob(RAMJob.class)
//                .withDescription("this is a ram job") //job的描述
//                .withIdentity("ramJob", "ramGroup") //job 的name和group
//                .build();
//
//        //任务运行的时间，SimpleSchedle类型触发器有效
//        long time=  System.currentTimeMillis() + 3*1000L; //3秒后启动任务
//        Date statTime = new Date(time);
//
//        //4.创建Trigger
//        //使用SimpleScheduleBuilder或者CronScheduleBuilder
//            String cal="";
//            double duration = warn.getDuration();
//
//            if (duration>0&&duration<60000){
//                cal="0/"+new Double(duration/1000).intValue() +" * * * * ?";
//            }else if (duration>=60000&&duration<60*60000){
//                cal="0 0/"+ new Double(duration/1000/60).intValue()+" * * * ?";
//            }else if (duration>=60*60000&&duration<24*60*60000){
//                cal="0 0 0/"+  new Double(duration/60/60000).intValue()+" * * ?";
//            }else if (duration>=24*60*60000){
//                cal="0 0 0 0/"+  new Double(duration/24/60/60000).intValue()+" * ?";
//            }
//        Trigger t = TriggerBuilder.newTrigger()
//                .withDescription("")
//                .withIdentity("ramTrigger", "ramTriggerGroup")
//                //.withSchedule(SimpleScheduleBuilder.simpleSchedule())
//                .startAt(statTime)  //默认当前时间启动
////                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?")) //两秒执行一次
//                .withSchedule(CronScheduleBuilder.cronSchedule(cal)) //两秒执行一次
//                .build();
//
//        //5.注册任务和定时器
//        scheduler.scheduleJob(jb, t);
//
//        //6.启动 调度器
//        scheduler.start();
////        _log.info("启动时间 ： " + new Date());
//
//        }catch (Exception e){
//            LOGGER.error("{}",e);
//        }
//
//    }
//
//}
