package com.thit.tibdm.rollup;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author: dongzhiquan  Date: 2018/12/24 Time: 17:23
 */
public class RollupUtilTest {

    private static final Logger LOGGER= LoggerFactory.getLogger(RollupUtilTest.class);

    @Test
    public void testQuery() throws Exception {


    }

    @Test
    public void testCreateTask() throws Exception {

//        RollupUtil.createTask("CY11","");

    }

    @Test
    public void testGetCount() throws Exception {

         RollupUtil.getCount("CY23","1701",System.currentTimeMillis(),100000);

    }
}