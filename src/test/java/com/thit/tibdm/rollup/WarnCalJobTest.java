package com.thit.tibdm.rollup;

import com.thit.tibdm.RedisUtil;
import com.thit.tibdm.parse.WarnVariable;
import org.junit.Test;
import org.redisson.api.RMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by dongzhiquan on 2019/3/1.
 */
public class WarnCalJobTest {

    @Test
    public void testCalFun() throws Exception {
        WarnVariable warn = new WarnVariable();
        List<String> func = new ArrayList<>();
        func.add("sum");
        warn.setFunc(func);
        List<String> paramList = new ArrayList<>();
        paramList.add("CY23");
        warn.setParam(paramList);
        warn.setDuration(100000);
        WarnCalJob warnCalJob = new WarnCalJob("pro", warn);
        String count = warnCalJob.calFun("range", paramList, "1701", warn);
        System.out.println("=== " + count);

        //average

    }

    @Test
    public void testEquals1() {
        WarnVariable warn = new WarnVariable();
        List<String> func = new ArrayList<>();
        func.add("sum");
        warn.setFunc(func);
        List<String> paramList = new ArrayList<>();
        paramList.add("CY23");
        warn.setParam(paramList);
        warn.setDuration(100000);
        WarnCalJob warnCalJob = new WarnCalJob("pro", warn);
        boolean getequals = getequals(paramList, "1701", warn);
        System.out.println(getequals);
    }


    public boolean getequals(List list) {
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


    public boolean getequals(List<String> paramList, String ch, WarnVariable warn) {
        boolean flag = false;
        String parm = paramList.get(0);
        List<String> countList = RollupUtil.getCount(parm, ch, System.currentTimeMillis(), new Double(warn.getDuration()).longValue());
        flag = getequals(countList);
        return flag;
    }

    @Test
    public void test() {

    }
}