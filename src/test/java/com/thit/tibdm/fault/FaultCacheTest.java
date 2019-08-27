package com.thit.tibdm.fault;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.thit.tibdm.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;


public class FaultCacheTest {
    /**
     * 测试故障诊断是否准确
     */
    @Test
    public void reportFault() {
        long curr = 1528939066591L;
        String machineId = "10000";
        ImmutableList<Integer> immutableList = ImmutableList.of(500, 1000, 1500, 2000);
        Map<Integer, Integer> gz1 = Maps.newHashMap();
        gz1.put(500, 15);
        gz1.put(1000, 4);
        gz1.put(1500, 2);
        gz1.put(2000, 2);
        Map<Integer, Integer> gz2 = Maps.newHashMap();
        gz2.put(500, 3);
        gz2.put(1000, 4);
        gz2.put(1500, 4);
        gz2.put(2000, 1);
        ImmutableList<String> gz1List = ImmutableList.of("0", "0", "1", "1", "1", "1", "0", "1", "0",
                "1", "1", "1", "0", "1", "1", "0", "1", "1", "0", "0", "1", "0", "0", "0", "0", "1", "0",
                "1", "1", "1", "1");
        ImmutableList<String> gz2List = ImmutableList.of("0", "0", "0", "1", "1", "1", "0", "0", "0",
                "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
                "0", "1", "1", "1");
        Map<String, String> statusMap = Maps.newHashMap();
        statusMap.put("CY1", "10");
        statusMap.put("CY2", "10");
        statusMap.put("CY3", "10");
        statusMap.put("CY23", "10");
        for (int i = 0; i < immutableList.size(); i++) {
            int count1 = 0;
            int count2 = 0;
            for (int j = 0; j < gz1List.size(); j++) {
                Map<String, String> gzMap = Maps.newHashMap();
                gzMap.put("COLLECT_TIME", String.valueOf(curr));
                gzMap.put("GZ1", gz1List.get(j));
                gzMap.put("GZ2", gz2List.get(j));

                List<Map<String, String>> fault1 = FaultCache.I.reportFault(Contents.FAULT, machineId, gzMap, statusMap, null);
                curr = curr + 500;
                if (fault1.size() != 0) {
                    for (Map<String, String> gzResut : fault1) {
                        String s = gzResut.get(Contents.BOM_CODE);
                        if (s.equals("GZ1")) {
                            count1++;
                        } else {
                            count2++;
                        }
                    }
                }


            }
            boolean flag1 = count1 == gz1.get(immutableList.get(i));
            boolean flag2 = count2 == gz2.get(immutableList.get(i));
            Assert.assertTrue(flag1);
            Assert.assertTrue(flag2);
        }

        RedisUtil.I.getRedisson().getMap(Contents.FAULT + machineId).clear();
    }

    @Test
    public void test() {
        String s = "{\"messageType\":\"TROUBLE\",\"content\":{\"flag\":\"WARN\",\"troubleArr\":[{\"BOM_TRAINNUMBER\":\"1702\",\"BOM_CODE\":\"GZ170001\",\"BOM_HANDLE\":\"0\",\"time\":\"1527228468401\"}]}}";
        System.out.println(s);
        s = s.replace("GZ", "");
        System.out.println(s);
    }
}