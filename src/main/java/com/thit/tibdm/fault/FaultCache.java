package com.thit.tibdm.fault;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thit.tibdm.Config;
import com.thit.tibdm.Contents;
import com.thit.tibdm.HttpUtil;
import com.thit.tibdm.RedisUtil;
import com.thit.tibdm.parse.WarnVariable;
import com.thit.tibdm.rollup.WarnCalJob;
import org.kairosdb.client.builder.MetricBuilder;
import org.redisson.api.RMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 故障缓存
 * 首先看时间是否大于上一次的状态
 *
 * @author wanghaoqiang
 */
public enum FaultCache {
    /**
     * 实例化
     */
    I;

//    public Map<String, Map<String, String>> fault_map;
    /**
     * 缓存的map
     */
    private Map<String, FaultEntity> faultCache;
    /**
     * 日志
     */
    private Logger logger;


    /**
     * 单例初始化
     */
    FaultCache() {
        faultCache = Maps.newConcurrentMap();
        logger = LoggerFactory.getLogger(FaultCache.class);
    }


    /**
     * 获取故障或者异常的cache
     *
     * @return
     */


    public Map<String, FaultEntity> getFaultCache() {
        return faultCache;
    }



    /**
     * 进行报告
     *
     * @return
     */
    public List<Map<String, String>> reportFault(String type, String machineId, Map<String, String> currgzMap, Map<String, String> messageMap, Map<String, WarnVariable> warnVariableMap) {

        currgzMap.putAll(WarnCalJob.getCurrgzMap());
        List<Map<String, String>> faultList = Lists.newArrayList();
        long curr = Long.parseLong(currgzMap.get(Contents.COLLECT_TIME));

        String cache_key = type + machineId;
        Map<String, Map<String, String>> fault = Maps.newHashMap();


        /**
         * 完成初始化
         * 假设数据不在redis,也不在缓存中
         */
        if (!faultCache.containsKey(cache_key)) {
            FaultEntity faultEntity = new FaultEntity();
            faultEntity.setTime(curr);
            RMap<String, String> last = RedisUtil.I.getRedisson().getMap(cache_key);

            if (last.size() == 0 || (currgzMap.size() - 1 != last.size())) {
                last.put(Contents.MACHINE_ID, machineId);
                last.put(Contents.COLLECT_TIME, String.valueOf(curr));
                currgzMap.forEach((k, v) -> {
                    if (k.startsWith(Contents.GZ)) {
                        faultEntity.getFaultMap().put(k, new FaultStatus(curr, 0));
                        last.put(k, String.valueOf(0));
                    }
                });
                logger.error("更新故障状态{}", machineId);

                faultCache.put(cache_key, faultEntity);
            } else {
                long time = Long.parseLong(last.get(Contents.COLLECT_TIME));
                last.readAllMap().forEach((k, v) -> {
                    if (k.startsWith(Contents.GZ)) {
                        faultEntity.getFaultMap().put(k, new FaultStatus(time, Integer.parseInt(v)));
                    }
                });

                faultCache.put(cache_key, faultEntity);
            }
        }

        /**
         * 1.当数据大于现在时间才有比较的价值
         * 2.当数据大于现在时间，上次数据相同的话，依然更新数据时间
         * 3.当数据大于现在时间，但是和上次不相同的话，先判断是否大于现在时间+我们所配置的数值，
         *   然后如果小于的话不做处理，如果大于的话则需要报出，并且更新时间，
         */
        FaultEntity lastCache = faultCache.get(cache_key);
        long time = Long.parseLong(currgzMap.get(Contents.COLLECT_TIME));
        if (time >= lastCache.getTime()) {
            Map<String, String> update = Maps.newHashMap();
            currgzMap.forEach((k, v) -> {
                Map<String, String> message = Maps.newHashMap();
                if (k.startsWith(Contents.GZ)) {
                    Map<String, FaultStatus> faultMap = lastCache.getFaultMap();
                    FaultStatus faultStatus;
                    if (faultMap.containsKey(k)) {
                        faultStatus = faultMap.get(k);
                    } else {
                        logger.info("执行重新构建方法：{}", k);
                        faultStatus = new FaultStatus(time, Integer.parseInt(v));
                        update.put(k, v);
                    }

//                    if (type.equals(Contents.FAULT)&&machineId.startsWith("17")){
//                        logger.info("============================"+JSON.toJSONString(currgzMap));
//                    }

                    /**
                     * 存在不相等的情况,大于故障时间才
                     * 过滤掉-1的情况，如果是-1的话，那么状态相当于没有变化
                     */
                    if ((!"-1".equals(v)) &&
                            (!v.equals(String.valueOf(faultStatus.getStatus())))) {
                        try {
                            if (type.equals(Contents.FAULT) && machineId.startsWith("17")) {
                                logger.info("============================change!!   {}", machineId);
                            }


                            double duration = Config.I.getFaultFrequency();
                            WarnVariable warnVariable = new WarnVariable();
                            if (Contents.WARN.equals(type)) {
                                warnVariable = warnVariableMap.get(k);
                                duration = warnVariable.getDuration();
                            }

                            if (time >= (faultStatus.getTime() + duration)) {
//
                                if (type.equals(Contents.WARN)) {
                                    k = k.replace(Contents.GZ, "");
//                                    if (k.contains("_")) {
//                                        k.replace("_", "-");
//                                    }
                                }
                                message.put(Contents.BOM_CODE, k);
                                message.put(Contents.BOM_HANDLE, v);
                                message.put(Contents.BOM_TRAINNUMBER, machineId);
                                message.put(Contents.TIME, String.valueOf(time));

                                fault.put(machineId, message);

                                if (messageMap != null && messageMap.size() != 0) {
                                    messageMap.forEach(message::put);
                                }

//                              if(machineId.startsWith("17")){
//                                  System.out.println(17);
//                              }

//                                if (machineId.equals("1719")){
//                                    System.out.println(123);
//                                }
                                if (Contents.FAULT.equals(type)) {
                                    faultList.add(message);
                                } else if (Contents.WARN.equals(type)) {

                                    if (warnVariable.isReport()) {
                                        faultList.add(message);
                                    }
//                                    if (warnVariable.getFunc()!=null){
//                                        save(k,machineId,time, JSON.toJSONString(message));
//                                    }
                                }

                                /**
                                 * 更新redis缓存和本地缓存
                                 */
                                faultStatus.setStatus(Integer.parseInt(v));
                                faultStatus.setTime(time);
                                update.put(k, v);
                            }

                        } catch (Exception e) {
                            logger.error("{}", e);
                        }

                    } else {
                        faultStatus.setTime(time);
                    }
                    lastCache.getFaultMap().put(k, faultStatus);
                }
            });
            lastCache.setTime(curr);
            if (update.size() != 0) {
                update.put(Contents.COLLECT_TIME, String.valueOf(time));
                RedisUtil.I.getRedisson().getMap(cache_key).putAll(update);
            }
        }
        /**
         * 最后更新到总的缓存
         */
        faultCache.put(cache_key, lastCache);
//        fault_map = fault;
        return faultList;
    }

    public void save(String metric, String machine_id, long time, String value) {
        MetricBuilder builder = MetricBuilder.getInstance();
        builder.addMetric(metric)
                .addTag("machine_id", machine_id)
                .addDataPoint(time, value);
        HttpUtil.sendKairosdb(builder);
    }
}
