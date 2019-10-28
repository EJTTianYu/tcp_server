package com.thit.tibdm.sender;

import com.thit.tibdm.HttpUtil;
import java.io.IOException;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KariosSenderThread extends Thread {

  private MetricBuilder builder;
  /**
   * 日志
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(KariosSenderThread.class);

  public KariosSenderThread(MetricBuilder builder) {
    this.builder = builder;
  }

  @Override
  public void run() {
    try {
      HttpUtil.getInstance().getClient().pushMetrics(builder);
    } catch (IOException e) {
      LOGGER.error("写入KairosDB失败,写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
    }
  }
}
