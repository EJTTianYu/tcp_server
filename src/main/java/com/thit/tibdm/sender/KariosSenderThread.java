package com.thit.tibdm.sender;

import com.thit.tibdm.HttpUtil;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KariosSenderThread extends Thread {

  private MetricBuilder builder;
  /**
   * 日志
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(KariosSenderThread.class);

  private static AtomicInteger threadCnt = new AtomicInteger(0);

  public KariosSenderThread(MetricBuilder builder) {
    this.builder = builder;
  }

  @Override
  public void run() {
    try {
      threadCnt.getAndIncrement();
      HttpUtil.getInstance().getClient().pushMetrics(builder);
      if (threadCnt.get() % 20 == 0) {
        LOGGER.info("数据写入Kairos成功,写入tag为:{}",
            builder.getMetrics().get(0).getTags().toString());
        threadCnt.set(0);
      }
    } catch (Exception e) {
      LOGGER.error("写入KairosDB失败,写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
    }
  }
}
