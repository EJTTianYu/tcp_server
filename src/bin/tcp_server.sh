#!/usr/bin/env bash
#获取脚本所在的路径,并且切换到主目录
BIN_DIR="${BASH_SOURCE-$0}"
BIN_DIR="$(dirname "${BIN_DIR}")"
BIN_DIR="$(cd "${BIN_DIR}"; pwd)"
HOME="$(cd "${BIN_DIR}";cd "../"; pwd)"
CFG_DIR="$HOME/conf"
LIB_DIR="$HOME/lib"
MAIN="$MAIN -jar $LIB_DIR/tcp_server-1.0-SNAPSHOT.jar"
echo "项目路径:"$HOME
echo "脚本路径:"$BIN_DIR
echo "jar包路径:"$BIN_DIR
echo "配置文件路径:"$CFG_DIR
cd $HOME

#设置java
if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

#设置classpath
CLASSPATH="$CFG_DIR:$CLASSPATH"

for i in "${LIB_PATH[@]}"
do
    CLASSPATH="$i:$CLASSPATH"
done

echo "CLASSPATH路径："$CLASSPATH

#创建临时文件目录
if [ "x${DATA_DIR}" = "x" ]
then
    DATA_DIR="${HOME}/tmp"
fi

#创建log目录
if [ "x${LOG_DIR}" = "x" ]
then
    LOG_DIR="${HOME}/logs"
fi

#pid文件存储
if [ -z "$PID_FILE" ]; then
#    MP_DATA_DIR="$($GREP "^[[:space:]]*dataDir" "$MP_CFG" | sed -e 's/.*=//')"
    if [ ! -d "$DATA_DIR" ]; then
        mkdir -p "$DATA_DIR"
    fi
    PID_FILE="$DATA_DIR/tcp_server.pid"
else
    # ensure it exists, otw stop will fail
    mkdir -p "$(dirname "$PID_FILE")"
fi
#1.netty的相关设置

#-Dio.netty.leakDetection.level
#netty的内存泄露检测分为四级：
#DISABLED: 不进行内存泄露的检测；
#SIMPLE: 抽样检测，且只对部分方法调用进行记录，消耗较小，有泄漏时可能会延迟报告，默认级别；
#ADVANCED: 抽样检测，记录对象最近几次的调用记录，有泄漏时可能会延迟报告；
#PARANOID: 每次创建一个对象时都进行泄露检测，且会记录对象最近的详细调用记录。是比较激进的内存泄露检测级别，消耗最大，建议只在测试时使用。

#-Dio.netty.selectorAutoRebuildThreshold=512 默认512
#在NIO中通过Selector的轮询当前是否有IO事件，根据JDK NIO api描述，Selector的select方法会一直阻塞，直到IO事件达到或超时，但是在Linux平台上这里有时会出现问题，在某些场景下select方法会直接返回，>即使没有超时并且也没有IO事件到达，这就是著名的epoll bug，这是一个比较严重的bug，它会导致线程陷入死循环，会让CPU飙到100%，极大地影响系统的可靠性，到目前为止，JDK都没有完全解决这个问题。
#但是Netty有效的规避了这个问题，经过实践证明，epoll bug已Netty框架解决，Netty的处理方式是这样的：
#记录select空转的次数，定义一个阀值，这个阀值默认是512，可以在应用层通过设置系统属性io.netty.selectorAutoRebuildThreshold传入，当空转的次数超过了这个阀值，重新构建新Selector，将老Selector上注册的Channel转移到新建的Selector上，关闭老Selector，用新的Selector代替老Selector，详细实现可以查看NioEventLoop中的selector和rebuildSelector方法：

#-Dio.netty.noKeySetOptimization
#是否禁用nio Selector.selectedKeys优化, 通过反射实现, 默认false

#JVM参数设置。如果需要另外添加jvm参数则新增具体的形式为
#JVM_FLAGS="$JVM_FLAGS -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8008"

#添加内存泄漏检测等级
JVM_FLAGS="-Dio.netty.leakDetection.level=advanced"
#添加远程调试功能
#JVM_FLAGS="$JVM_FLAGS -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8008"
#GC设置，包括最大最小，以及使用的垃圾回收器
JVM_FLAGS="$JVM_FLAGS -server -Xmx1024m -Xms1024m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
#GC日志 发生OOM时创建堆内存转储文件
JVM_FLAGS="$JVM_FLAGS -Xloggc:$LOG_DIR/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"



case $1 in
start)
    echo  -n "Starting... "
    if [ -f "$PID_FILE" ]; then
      if kill -0 `cat "$PID_FILE"` > /dev/null 2>&1; then
         echo server already running as process `cat "$PID_FILE"`.
         exit 0
      fi
    fi
    nohup "$JAVA" -cp "$CLASSPATH" $JVM_FLAGS $MAIN > server.log 2>&1 < /dev/null &
    if [ $? -eq 0 ]
    then
      case "$OSTYPE" in
      *solaris*)
        /bin/echo "${!}\\c" > "$PID_FILE"
        ;;
      *)
        /bin/echo -n $! > "$PID_FILE"
        ;;
      esac
      if [ $? -eq 0 ];
      then
        sleep 1
        echo STARTED
      else
        echo FAILED TO WRITE PID
        exit 1
      fi
    else
      echo SERVER DID NOT START
      exit 1
    fi
    ;;
start-foreground)
    echo  -n "Starting ... "
    "$JAVA" -cp "$CLASSPATH" $JVM_FLAGS $MAIN
    ;;
stop)
    echo "Stopping tcp_server ... "
    if [ ! -f "$PID_FILE" ]
    then
      echo "no tcp_server to stop (could not find file $PID_FILE)"
    else
      kill -15 $(cat "$PID_FILE")
      SLEEP=30
      SLEEP_COUNT=1
      while [ $SLEEP -ge 0 ]; do
        kill -0 $(cat "$PID_FILE") >/dev/null 2>&1
        if [ $? -gt 0 ]; then
          rm -f "$PID_FILE" >/dev/null 2>&1
          if [ $? != 0 ]; then
            if [ -w "$PID_FILE" ]; then
              cat /dev/null > "$PID_FILE"
            else
              echo "The PID file could not be removed or cleared."
            fi
          fi
          echo STOPPED
          break
        fi
        if [ $SLEEP -gt 0 ]; then
          echo "stopping ... $SLEEP_COUNT"
          sleep 1
        fi
        if [ $SLEEP -eq 0 ]; then
          echo "MPUSH did not stop in time."
          echo "To aid diagnostics a thread dump has been written to standard out."
          kill -3 `cat "$PID_FILE"`
          echo "force stop MPUSH."
          kill -9 `cat "$PID_FILE"`
          echo STOPPED
        fi
        SLEEP=`expr $SLEEP - 1`
        SLEEP_COUNT=`expr $SLEEP_COUNT + 1`
      done
    fi
    exit 0
    ;;
restart)
    shift
    "$0" stop ${@}
    sleep 1
    "$0" start ${@}
    ;;
esac