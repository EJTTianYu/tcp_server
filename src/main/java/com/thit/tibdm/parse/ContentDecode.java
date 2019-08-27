package com.thit.tibdm.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.thit.tibdm.*;
import com.thit.tibdm.fault.FaultCache;
import com.thit.tibdm.fault.FaultEntity;
import com.thit.tibdm.fault.FaultStatus;
//import com.thit.tibdm.rollup.RollupUtil;
import com.thit.tibdm.rollup.RollupUtil;
import com.thit.tibdm.rollup.WarnCalJob;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 测试解码效率
 */
public enum ContentDecode {
    /**
     * 实例化
     */
    I;

    /**
     * redis推送通道
     */
    private static final String PRO_UPDATA_CHANNEL = "pro_update_ok";
    /**
     * 报表的监控名字
     */
    private static final String PARSE_TIME = "parse.time";
    /**
     * redis存储的协议key
     */
    private static final String PRO_DETAIL = "PROALL";
    /**
     * redis存储的协议的hashkey
     */
    private static final String ALL = "all";
    /**
     * 日志
     */
    private Logger logger;
    /**
     * 为了兼容分包的情况需要对其做一定的修改，
     * machineMapperPro key为 车号_设备号 value 不变
     */
    private Map<String, String> machineMapperPro;
    /**
     * 协议的对照类
     */
    private Map<String, ParseRule> ruleMap;
    /**
     * 协议以及车号的对照
     */
    private Map<String, List<String>> proMachinds;
    /**
     * 故障的缓存
     */
    private Map<String, FaultEntity> faultCache;

    private Map<String, String> warnMap;

    /**
     * 单例初始化生成协议解析类
     */
    ContentDecode() {
        logger = LoggerFactory.getLogger(ContentDecode.class);
        machineMapperPro = Maps.newConcurrentMap();
        ruleMap = Maps.newConcurrentMap();
        proMachinds = Maps.newHashMap();
        warnMap = Maps.newConcurrentMap();
        loadPro();
        ThreadPoolManager.I.getConnectMonitorTimer().scheduleAtFixedRate(new ProJob(), 1, 60, TimeUnit.SECONDS);
    }

    public Map<String, String> getMachineMapperPro() {
        return machineMapperPro;
    }

    public Map<String, ParseRule> getRuleMap() {
        return ruleMap;
    }

    /**
     * 加载协议
     * 在监听到更新请求的时候去重新载入协议
     */
    private void loadPro() {
        loadProDetail();
        RTopic<String> proChannel = RedisUtil.I.getRedisson().getTopic(PRO_UPDATA_CHANNEL);
        proChannel.addListener((channel, message) -> {
            logger.info("更新{}协议......", message);
            loadProDetail();
        });
    }

    /**
     * 载入详细的协议信息
     */
    private void loadProDetail() {
        RMap<String, String> map = RedisUtil.I.getRedisson().getMap(PRO_DETAIL);
        try {
            transfromPro(map.get(ALL));
        } catch (Exception e) {
            logger.error("发生协议异常：{}", e);
        }
    }

    /**
     * 协议解析转换类
     *
     * @param pro
     */
    private void transfromPro(String pro) {
        JSONObject parse = (JSONObject) JSON.parse(pro);
        String data1 = parse.getString("data");
        JSONObject data2 = (JSONObject) JSON.parse(data1);
        Map<String, JSONObject> proMap = (Map<String, JSONObject>) JSON.parse(data2.getString("variable"));
        Map<String, JSONObject> chMap = (Map<String, JSONObject>) JSON.parse(data2.getString("vehclePRo"));
        //先进行车号和协议信息的清空，然后再进行车号的放入
        Map<String, String> machineMapperProTmp = Maps.newConcurrentMap();
        Map<String, List<String>> proMachindsTmp = Maps.newConcurrentMap();
        chMap.forEach((k, v) -> {
            machineMapperProTmp.put(k, v.getString("EafCode"));
            if (proMachindsTmp.containsKey(v.getString("EafCode"))) {
                List<String> machines = proMachindsTmp.get(v.getString("EafCode"));
                machines.add(k);
            } else {
                List<String> machines = Lists.newArrayList();
                machines.add(k);
                proMachindsTmp.put(v.getString("EafCode"), machines);
            }
        });
        machineMapperPro = machineMapperProTmp;
        proMachinds = proMachindsTmp;
        proMap.forEach((k, v) -> {
            /**
             * 假设协议为分包发布的话，则多传输一个包头包尾，
             * 将其分成两个协议，
             * 分包标志：0为不分包，1为分包
             */
            try {
                ParseRule parseRule = initPro(k, v);
                ruleMap.put(k, parseRule);
                logger.info("解析协议：{},发送模式为：{}", k, parseRule.getSendMode());
            } catch (Exception e) {
                logger.error(k + "解析协议异常：{}", e);
            }
        });
    }

    /**
     * 生成协议
     *
     * @param pro
     * @return
     */
    private ParseRule initPro(String proid, JSONObject pro) {

        List<Variable> variables = Lists.newArrayList();
        List<VirtualVariable> virtualVariables = Lists.newArrayList();
        List<WarnVariable> warnlVariables = Lists.newArrayList();
        /**
         * 将参数里面头包和尾包变量分割开来
         * 分别放入两个包里面
         */
        try {
            JSONArray array1 = JSON.parseArray(pro.getString("Variable"));
            array1.forEach(item -> {
                JSONObject object = (JSONObject) item;
                Variable variable = new Variable(object.getString("UniqueCode"),
                        "Y".equals(object.getString("IsSigned")),
                        Integer.parseInt(object.getString("ByteOffset")),
                        Integer.parseInt(object.getString("BitOffset")),
                        Integer.parseInt(object.getString("ByteLength")),
                        Integer.parseInt(object.getString("BitLength")),
                        object.getString("Conversion"),
                        object.getString("Type")
                );
                int deviceId = getDeviceId(object);
                variable.setDeviceId(deviceId);
                variables.add(variable);
            });
        } catch (Exception e) {
            logger.error("基础变量协议错误{}", e);
        }
        try {
            JSONArray array2 = JSON.parseArray(pro.getString("VirtualVariable"));
            array2.forEach(item -> {
                JSONObject object = (JSONObject) item;
                VirtualVariable virtualVariable = new VirtualVariable();
                int deviceId = getDeviceId(object);
                virtualVariable.setDeviceId(deviceId);
                virtualVariable.setName(object.getString("UniqueCode"));
                virtualVariable.setCalcuAccuracy("".equals(object.getString("CalcuAccuracy")) ? "0" : object.getString("CalcuAccuracy"));
                Map<String, List<String>> conversion = (Map<String, List<String>>) JSON.parse("{" + object.getString("Conversion") + "}");
                conversion.forEach((k, v) -> {
                    virtualVariable.setConversion(k);
                    virtualVariable.setParam(v);
                });
                virtualVariables.add(virtualVariable);
            });
        } catch (Exception e) {
            logger.error("虚拟变量协议错误{}", e);
        }
        try {
            JSONArray array3 = JSON.parseArray(pro.getString("WarnVariable"));
            if (array3 != null && array3.size() != 0) {
                array3.forEach(item -> {
                    JSONObject object = (JSONObject) item;
                    WarnVariable warnVariable = new WarnVariable();
                    String serialNumber = object.get("SerialNumber").toString();
                    warnVariable.setSerialNumber(serialNumber);
                    String jexl = "";
                    if (object.get("Jexl") != null) {
                        jexl = object.get("Jexl").toString();
                    }
                    warnVariable.setJexl(jexl);
                    if (object.get("Report") != null) {
                        warnVariable.setReport("true".equals(object.get("Report").toString()) ? true : false);
                    }
                    if (object.get("Func") != null) {
                        List<String> list = (List<String>) object.get("Func");
                        warnVariable.setFunc(list);
                    }

                    if (object.containsKey("Duration")) {
                        warnVariable.setDuration(Double.parseDouble(object.get("Duration").toString()) * 1000);
                    } else {
                        logger.error("未对异常变量进行配置默认1秒");
                        warnVariable.setDuration(1000);
                    }
                    warnVariable.setParam((List<String>) object.get("Param"));
                    int deviceId = getDeviceId(object);
                    warnVariable.setDeviceId(deviceId);
                    warnlVariables.add(warnVariable);
                });
            }
        } catch (Exception e) {
            logger.error("告警协议错误{}", e);
        }
        //更新聚合任务
        try {
//            if (warnlVariables.size() != 0){
//                RollupUtil.updateTasks(warnlVariables);
//            }

            //每次检查一下 变量内容  如果不一致 更新
            //如果需要更新 执行下面

            //pro  name warn
            //如果没有 直接新建
            //如果有 判断一下 删除更新
            for (WarnVariable warn : warnlVariables) {
                if (warn.getFunc() != null && warn.getFunc().size() != 0) {
                    Gson gson = new GsonBuilder().disableHtmlEscaping().create();
                    String json = gson.toJson(warn);
                    if (warn.getDuration()==0){
                        continue;
                    }
                    int i = new Double(warn.getDuration()).intValue() / 2;
                    //如果线程执行时间小于10秒 则默认10秒  如果大于60秒 则设为60秒
                    if (i<=10){
                        i=10;
                    }
                    if (i>60){
                        i=60;
                    }
                    if (!warnMap.containsKey(proid + "_" + warn.getSerialNumber())) {
                        ThreadPoolManager.I.getConnectMonitorTimer().scheduleAtFixedRate(new WarnCalJob(proid, warn), 1, i, TimeUnit.MILLISECONDS);
                        logger.info("生成新的定时任务!");
                        warnMap.put(proid + "_" + warn.getSerialNumber(), json);
                    } else {
                        String value = warnMap.get(proid + "_" + warn.getSerialNumber()).toString();
                        if (!value.equals(json)) {
                            //需要更新   先把原来的停掉  然后再添加
                            ThreadPoolManager.I.getConnectMonitorTimer().scheduleAtFixedRate(new WarnCalJob(proid, warn), 1, i, TimeUnit.MILLISECONDS);
                            logger.info("生成新的定时任务!同时需要关闭旧的定时任务");
                            Map<String, String> map1 = new HashMap<>();
                            map1.put(warn.getSerialNumber(), "stop");
                            RedisUtil.I.getRedisson().getMap("warnstop_" + proid).putAll(map1);
                        }
                    }

                }
            }

        } catch (Exception e) {
            logger.error("发生错误{}", e);
        }
        List<String> special = new ArrayList<>();
        try {
            JSONArray array4 = JSON.parseArray(pro.getString("SpecialVariable"));
            if (array4 != null && array4.size() != 0) {
                array4.forEach(item -> special.add(item.toString()));
            }
        } catch (Exception e) {
            logger.error("特殊变量错误{}", e);
        }

        int proMode = 0;
        if (pro.containsKey("SendMode")) {
            String sendMode = pro.getString("SendMode");
            if ("DOUBLE".equals(sendMode)) {
                proMode = 1;
            }
        }
        return new ParseRule(variables, virtualVariables, warnlVariables, special, proMode);
    }


    /**
     * 在协议的变量里面，如果是单包发送那么设备ID默认为1
     * 如果是分包发送，依据判断条件知道设备ID是1或者2
     *
     * @param map
     * @return
     */
    private int getDeviceId(JSONObject map) {
        int deviceId = 1;
        if (map.containsKey("BelongPack")) {
            String belongPack = map.getString("BelongPack");
            if (!"HEAD".equals(belongPack)) {
                deviceId = 2;
            }
        }
        return deviceId;
    }


    /**
     * 需要在解析的时候知道这个协议是什么协议，
     * 如果是单包协议的话，那么就不要执行变量过滤的功能，需要对所有的变量进行解析，
     * 如果是分包协议的话，那么就需要对协议的数据包进行甄别，如果是1设备的那么就只进行1设备变量的解析
     *
     * @param bytes
     * @return
     */
    public Map<String, Object> parse(byte[] bytes, int deviceId) {
        long start = System.currentTimeMillis();

        Timer timer = MetricMonitor.I.registryTimer(PARSE_TIME);
        Timer.Context context = timer.time();
        Map<String, Object> map = new HashMap<>(8);
        Map<String, String> gzMap = new HashMap<>(8);
        Map<String, String> statusMap = new HashMap<>(8);
        Map<String, String> warnMap = new HashMap<>(8);
        Map<String, String> messageMap = new HashMap<>(8);
        String ch = HexUtil.converByteToV(bytes, 0, 2, false) + "";
        String time = HexUtil.converByteToV(bytes, 2, 8, true) + "";
        if (machineMapperPro.containsKey(ch)) {
            String proId = machineMapperPro.get(ch);
            if (ruleMap.containsKey(proId)) {
                gzMap.put("MACHINE_ID", ch);
                gzMap.put("CH", ch);
                gzMap.put("COLLECT_TIME", time + "");
                setDeviceTime(gzMap, deviceId, time);
                statusMap.put("MACHINE_ID", ch);
                statusMap.put("COLLECT_TIME", time + "");
                statusMap.put("CH", ch);
                setDeviceTime(statusMap, deviceId, time);
                warnMap.put("MACHINE_ID", ch);
                warnMap.put("CH", ch);
                warnMap.put("COLLECT_TIME", time + "");
                setDeviceTime(warnMap, deviceId, time);
                ParseRule rule = ruleMap.get(proId);
                for (Variable variable : rule.getVariables()) {
                    /**
                     * 如果单包不需要进行分别
                     * 如果是分包，则需要设备号和变量的设备号进行匹配，匹配上了才可以
                     */
                    if (isContinue(rule, variable.getDeviceId(), deviceId)) {
                        parseVariable(gzMap, statusMap, variable, bytes);
                    }
                }

                boolean isSingle = rule.getSendMode() == 0;
                List<VirtualVariable> virtualList = rule.getVirtualVariables();
                if (virtualList != null) {
                    for (VirtualVariable virtual : virtualList) {
                        if (!"".equals(virtual.getName()) && !"x".equals(virtual.getConversion())) {
                            if (isContinue(rule, virtual.getDeviceId(), deviceId)) {
                                parseVir(virtual.getConversion(),
                                        virtual.getParam(),
                                        statusMap,
                                        virtual.getCalcuAccuracy(),
                                        virtual.getName(),
                                        ch,
                                        gzMap,
                                        statusMap,
                                        isSingle, null);
                            }
                        }
                    }
                }

                /**
                 * 异常变量
                 */
                Map<String, WarnVariable> warnVari_Map = new HashMap<String, WarnVariable>();
                List<WarnVariable> warnVaribales = rule.getWarnVaribales();
                faultCache = FaultCache.I.getFaultCache();
                if (warnVaribales != null) {
                    for (WarnVariable warn : warnVaribales) {
                        String jexl = warn.getJexl();
                        warnVari_Map.put(Contents.GZ + warn.getSerialNumber(), warn);
                        if (isContinue(rule, warn.getDeviceId(), deviceId)) {
                            if (!jexl.contains("&&") && !jexl.contains("||")) {
                                if (warn.getFunc()!=null&&warn.getDuration()>0){
                                    //需要定时计算的异常变量 不需要实时计算
                                    continue;
                                }
                                parseVir(warn.getJexl(),
                                        warn.getParam(),
                                        warnMap,
                                        null,
                                        Contents.GZ + warn.getSerialNumber(),
                                        ch,
                                        gzMap,
                                        statusMap,
                                        isSingle, warn);
                            }


                            List<String> param = warn.getParam();
                            if (jexl.contains("&&") || jexl.contains("||")) {
                                JexlContext mapJexl = new MapContext();
                                int status = -1;
                                for (int i = 0; i < param.size(); i++) {
                                    String code = param.get(i);
                                    WarnVariable warnVariable = warnVari_Map.get(Contents.GZ + code);
                                    FaultEntity faultEntity = faultCache.get(Contents.WARN + ch);
                                    if (faultEntity != null) {
                                        Map<String, FaultStatus> faultMap = faultEntity.getFaultMap();
                                        if (faultMap != null) {
                                            FaultStatus faultStatus = faultMap.get(Contents.GZ + code);
                                            if (faultStatus != null) {
                                                if ((Long.parseLong(time) - faultStatus.getTime()) >= warnVariable.getDuration()) {
                                                    status = faultStatus.getStatus();
                                                    if (code.contains("-")) {
                                                        code = code.replace("-", "_");
                                                    }
                                                    mapJexl.set(code, status);
                                                }
                                            }
                                        }

                                    }
                                }
                                if (jexl.contains("-")) {
                                    jexl = jexl.replace("-", "_");
                                }
                                String re = JexlPool.I.doConvert(jexl, mapJexl);
                                if ("true".equals(re)) {
                                    warnMap.put(Contents.GZ + warn.getSerialNumber(), "1");
                                }
                                if ("false".equals(re)) {
                                    warnMap.put(Contents.GZ + warn.getSerialNumber(), "0");
                                }

                            }
                        }
                    }
                }

                /**
                 *
                 */
                List<String> specialList = rule.getSpecialVariable();
                if (specialList != null && specialList.size() != 0) {
                    specialList.forEach(item -> {
                        messageMap.put(item, statusMap.get(item));
                        if (statusMap.get(item) == null) {
                            messageMap.put(item, "");
                        }
                    });
                }
                map.put("MACHINE_ID", ch);
                map.put("GZ_MAP", gzMap);
                map.put("ORTHER_MAP", statusMap);
                map.put("MESSAGE_MAP", messageMap);
                map.put("WARN_MAP", warnMap);
                map.put(Contents.warnVari_Map, warnVari_Map);
            } else {
                logger.error("车辆{}找不到协议！！", ch);
            }
        }
        long end = System.currentTimeMillis();
        if (end - start > 100) {
            logger.error("{}车解析时间过长,时间为{}", ch, (end - start));
        }
        context.stop();
        return map;
    }

    /**
     * 计算虚拟变量以及异常变量
     * 增加对虚拟变量有双设备的数据的支持：当所需变量不存在与这个数据包中的时候，去缓存中找到这个数据包
     *
     * @param jexl          jexl表达式
     * @param param         jexl所需要的参数列表
     * @param gzMap         jexl计算所需要的参数来源
     * @param statusMap     参数来源
     * @param ch            车号
     * @param resultMap     计算完的结果放的位置 相当于状态map和异常map
     * @param calcuAccuracy 是否进行精度保留
     */
    private void parseVir(String jexl,
                          List<String> param,
                          Map<String, String> resultMap,
                          String calcuAccuracy,
                          String name,
                          String ch,
                          Map<String, String> gzMap,
                          Map<String, String> statusMap,
                          boolean isSingle,
                          WarnVariable warnVariable) {

        String format = "";
        JexlContext mapjexl = new MapContext();
        try {
            boolean needDouble = false;
            if (jexl.contains(">")
                    || jexl.contains("<")
                    || jexl.contains("==")) {
                needDouble = true;
            }
            /**
             *增加字符串变量
             */
            boolean finalNeedDouble = needDouble;
            List list = new ArrayList();
            param.forEach(m -> {
                String value;
                if (m.startsWith("GZ")) {
                    if (isSingle) {
                        value = gzMap.getOrDefault(m, "0");
                    } else {
                        if (gzMap.containsKey(m)) {
                            value = gzMap.get(m);
                        } else {
                            RMap<String, String> map = RedisUtil.I.getRedisson().getMap(Contents.FAULT + ch);
                            value = map.getOrDefault(m, "0");
                            logger.info("去redis中取{},表达式是{}", m, param.toString());
                        }
                    }
                } else {
                    if (isSingle) {
                        value = statusMap.getOrDefault(m, "1");
                    } else {
                        if (!param.contains("YC")){
                            if (statusMap.containsKey(m)) {
                                value = statusMap.get(m);
                            } else {
                                logger.info("去redis中取：{}", m);
                                RMap<String, String> map = RedisUtil.I.getRedisson().getMap(Contents.STATUS_PREFIX + ch);
                                value = map.getOrDefault(m, "0");
                            }
                        }else {
                            RMap<String, String> map = RedisUtil.I.getRedisson().getMap(Contents.WARNCAL_ + ch);
                            value=map.getOrDefault(m, "0");
                        }

                    }
                }
                if (finalNeedDouble) {
                    mapjexl.set(m, Double.parseDouble(value));
                } else {
                    mapjexl.set(m, value);
                }

                if (warnVariable != null && warnVariable.getFunc() != null) {
                    list.add(mapjexl.get(m));
                }
            });
            if (warnVariable != null && warnVariable.getFunc() != null&&warnVariable.getDuration()==0) {
                    String s = warnVariable.getFunc().get(0);
                    format = calFun(s, list);
                    name = name.replace(Contents.GZ, "");

            } else {
                format = JexlPool.I.doConvert(jexl, mapjexl);
            }


            if (calcuAccuracy != null) {
                if (!"".equals(calcuAccuracy)) {
                    try {
                        if (!"true".equals(format) && !"false".equals(format)) {
                            format = String.format("%." + calcuAccuracy + "f", Double.parseDouble(format));
                        }
                    } catch (Exception e) {
                        //过滤掉即可，因为部分是String
                    }
                }
            } else {
                if ("true".equals(format)) {
                    format = "1";
                } else if ("false".equals(format)) {
                    format = "0";
                }
            }
            resultMap.put(name, format);
        } catch (Exception e) {
            resultMap.put(name, "-1");
            logger.error("异常原因：{}", e);
            logger.error("额外信息：表达式{},输入参数{}", jexl, mapjexl.toString());
        }
    }

    /**
     * 根据协议来确定是否需要根据多数据包来过滤
     *
     * @return
     */
    public boolean isSinglePacket(int ch) {
        boolean result = true;
        if (machineMapperPro.containsKey(String.valueOf(ch))) {
            String proId = machineMapperPro.get(String.valueOf(ch));
            if (ruleMap.containsKey(proId)) {
                ParseRule parseRule = ruleMap.get(proId);
                if (parseRule.getSendMode() != 0) {
                    result = false;
                }
            }
        }
        return result;
    }


    public String calFun(String fun, List list) {
        String cal = "";
        double num = 0;
        switch (fun) {
            case "average":
                for (Object str : list) {
                    num += Double.parseDouble(str.toString());
                }
                cal = num / list.size() + "";
                break;
            case "sum":
                for (Object str : list) {
                    num += Double.parseDouble(str.toString());
                }
                cal = num + "";
                break;
            case "range":
                cal = (getMax(list) - getMin(list)) + "";
                break;
            case "uniformity":
                //true返回1  false返回0
                cal = getequals(list) + "";
                break;
        }

        return cal;
    }

    public static boolean getequals(List list) {
        boolean flag = false;
        Object obj = new Object();
        if (list != null && list.size() != 0) {
            obj = list.get(0);
        } else {
            return false;
        }
        for (Object o : list) {
            if (!obj.equals(o)) {
                return flag;
            }
        }
        flag = true;
        return flag;
    }

    public static double getMax(List list) {
        double num = 0;
        for (Object str : list) {
            double v = Double.parseDouble(str.toString());
            if (v >= num) {
                num = v;
            }
        }
        return num;
    }

    public static double getMin(List list) {
        double num = 0;
        for (Object str : list) {
            double v = Double.parseDouble(str.toString());
            if (v <= num) {
                num = v;
            }
        }
        return num;
    }


    /**
     * 解析故障变量和状态变量
     *
     * @param gzMap
     * @param statusMap
     * @param variable
     * @param bytes
     */
    private void parseVariable(Map<String, String> gzMap,
                               Map<String, String> statusMap,
                               Variable variable,
                               byte[] bytes) {
        String parseResult = "";
        try {
            int byteOffset = variable.getByteOffset();
            int offset = byteOffset + 10;
            //排除预留字节 预留的字节位移为-1
            if (offset != -1) {
                //截取16进制字节
                //一个字节
                if (variable.getByteLength() == 1) {
                    if (!variable.isSign()) {
                        parseResult = String.valueOf(HexUtil.getUnByte(bytes[offset]));
                    } else {
                        parseResult = String.valueOf(bytes[offset]);
                    }
                    if (variable.getBitLength() != 8) {
                        //一个字节位
                        //一个字节
                        int a = variable.getBitOffset();
                        int b = variable.getBitOffset() + variable.getBitLength();
                        parseResult = HexUtil.getBitsByByte(bytes[offset], a, b);
                    }
                } else if (variable.getByteLength() > 1) {
                    //无符号
                    if (!variable.isSign()) {
                        if (variable.getByteLength() == 2) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 2, false);
                        } else if (variable.getByteLength() == 3) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 3, false);
                        } else if (variable.getByteLength() == 4) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 4, false);
                        }
                    } else {
                        if (variable.getByteLength() == 2) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 2, true);
                        } else if (variable.getByteLength() == 3) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 3, true);
                        } else if (variable.getByteLength() == 4) {
                            parseResult = HexUtil.converByteToV(bytes, offset, 4, true);
                        }
                    }
                }
                //全量数据里面故障是tinyint类型
                if (!"STATE".equals(variable.getType())) {
                    gzMap.put(variable.getName(), parseResult);
                } else {
                    if (!"x".equals(variable.getConversion())) {
                        JexlContext jexlContext = new MapContext();
                        String jexlExp = variable.getConversion();
                        jexlContext.set("x", parseResult);
                        parseResult = JexlPool.I.doConvert(jexlExp, jexlContext);
                    }
                    statusMap.put(variable.getName(), parseResult);
                }
            }

        } catch (Exception e) {
            //故障变量入库为int类型
            if ("TROUBLE".equals(variable.getType())) {
                gzMap.put(variable.getName(), "-1");
            } else {
                statusMap.put(variable.getName(), "-1");
            }
            logger.error("发生解析异常：{}车{}变量",
                    statusMap.get("MACHINE_ID"),
                    variable.getName());
        }
    }

    /**
     * 判断变量是否继续进行解析操作
     *
     * @param rule
     * @param variableDeviceId
     * @param deviceId
     * @return
     */
    private boolean isContinue(ParseRule rule, int variableDeviceId, int deviceId) {
        return (rule.getSendMode() == 0) || (variableDeviceId == deviceId);
    }

    /**
     * 或者去包头还是包尾
     * @param map
     * @param deviceId
     * @param time
     */
    private void setDeviceTime(Map<String, String> map, int deviceId, String time) {
        if (deviceId == 1) {
            map.put(Contents.HEAD_TIME, time);
        } else {
            map.put(Contents.TAIL_TIME, time);
        }
    }

    public boolean isRecord(long ch) {
        if (machineMapperPro.containsKey(String.valueOf(ch))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取协议和车号的对照
     *
     * @return
     */
    public Map<String, List<String>> getProMachinds() {
        return proMachinds;
    }

    /**
     * 协议定时载入类
     */
    class ProJob implements Runnable {
        @Override
        public void run() {
            logger.info("协议开始定时更新....");
            loadProDetail();
        }
    }
}
