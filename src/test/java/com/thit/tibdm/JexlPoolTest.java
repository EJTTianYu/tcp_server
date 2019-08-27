package com.thit.tibdm;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.junit.Assert;
import org.junit.Test;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class JexlPoolTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JexlPoolTest.class);

    private String conditionalIf = "if ((x * 2) == 6) {\n" +
            "                            return 1;\n" +
            "                     } else {\n" +
            "                            return 2;\n" +
            "                     }";
    private JexlContext jc = new MapContext();

    private String test = "((ZT30004==0)&&(ZT30003==1))?1:0";

    @Test
    public void doConvert() {
        JexlContext jc = new MapContext();
        Double tmp = 53.00;
        jc.set("CY29", tmp);
        LOGGER.info(JexlPool.I.doConvert("CY29 == 88", jc));
//        long l = Long.parseLong(tmp);
    }

    @Test
    public void testIf() {
        jc.set("x", 3);
        Assert.assertEquals("1", JexlPool.I.doConvert(conditionalIf, jc));
    }

    @Test
    public void testKuohao() {
        jc.set("ZT30004", "0");
        jc.set("ZT30003", "1");
        Assert.assertTrue("1".equals(JexlPool.I.doConvert(test, jc)));
    }


    @Test
    public void testString() {
        String jexl = "(ZT1363/32)+\" .  \"+((ZT1363%32)/4)+\".\"+(ZT1363/4)";
        jc.set("ZT1363", 100);
        LOGGER.info(JexlPool.I.doConvert(jexl, jc));
    }

    @Test
    public void testReplace() {
        String test = "hello world";
        String replace = test.replace(" ", "");
        String substring = test.substring(1, 2);
        LOGGER.info("结果：{}", replace);
        LOGGER.info("结果：{}", substring);
    }

    @Test
    public void testCF() {
        String jexl = "(ZT10525+ZT10526+ZT10527+ZT10528)*0.25";
        jc.set("ZT10525", 147);
        jc.set("ZT10526", 140);
        jc.set("ZT10527", 139);
        jc.set("ZT10528", 141);
        LOGGER.info(JexlPool.I.doConvert(jexl, jc));
    }
    @Test
    public void test_(){
        String jexl="YC1_1&&YC1_2";
        jc.set("YC1_1",0);
        jc.set("YC1_2",1);
        String s = JexlPool.I.doConvert(jexl, jc);
        LOGGER.info(s);
    }
}