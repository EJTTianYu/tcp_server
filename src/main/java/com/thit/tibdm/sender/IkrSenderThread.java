package com.thit.tibdm.sender;

import com.thit.tibdm.HttpUtil;
import java.io.IOException;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IkrSenderThread extends Thread {

  private MetricBuilder builder;
  /**
   * 日志
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(IkrSenderThread.class);

  public IkrSenderThread(MetricBuilder builder) {
    this.builder = builder;
  }

  @Override
  public void run() {
    try {
      HttpUtil.getInstance().getIkrClient().pushMetrics(builder);
    } catch (IOException e) {
      LOGGER.error("写入IKR失败,写入tag为:{}", builder.getMetrics().get(0).getTags().toString());
    }
  }
}