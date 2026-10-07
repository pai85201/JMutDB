package cainsgl.core.persistence.RDB;

import cainsgl.core.config.MutConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/* 监听器 - 监听插入次数
*  两种方案：监听器执行RDB流程；独立线程执行RDB流程
*  1. 第一种方案适合业务访问数据库频率低；按分钟级别执行RDB
*  2. 第二种则相反；按秒级别执行RDB；通过优化线程任务队列来达到任务的精确执行
*  */
public class RDBListener {

    private static final Logger log = LoggerFactory.getLogger(RDBListener.class);

    // 计数器；增强多线程共同累加计数器的性能
    private static final LongAdder counter = new LongAdder();

    // 检查计数器的单位间隔时间
    private static final long intervalTime = MutConfiguration.RDB.INTERVAL_TIME;
    // 单位时间内触发RDB的最小插入数据次数
    private static final long insertCount = MutConfiguration.RDB.INSERT_COUNT;
    // 最大延迟次数
    private static final int maxDelayCount = MutConfiguration.RDB.MAX_DELAY_COUNT;
    // 业务类型
    private static final String businessType = MutConfiguration.RDB.BUSINESS_TYPE;

    // 定时任务
    private static final ScheduledExecutorService scheduleExecutor = Executors.newSingleThreadScheduledExecutor();
    // 高频业务场景下用于直接执行RDB逻辑的线程
    private static ExecutorService rdbExecutor;

    // 任务延时的次数
    private static final AtomicInteger delayCount = new AtomicInteger(0);
    // 延迟任务
    private static volatile ScheduledFuture<?> currentTask;

    static {
        log.debug("RDBListener initialized.");
        init();
        // 如果场景为高频场景；创建独立线程
        if("HIGH".equals(businessType)) {
            rdbExecutor = Executors.newSingleThreadExecutor();

            // 关闭用Hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                forceExecuteRDB();
                shutdownExecutor(rdbExecutor);
            }));
        }
    }

    // 开启周期定时任务；每隔一个时间单位检查一次计数器；如果计数器大于等于阈值则触发RDB
    private static void init(){
        scheduleExecutor.scheduleAtFixedRate(() -> {
            long count = counter.longValue();
            log.info("RDBListener executed in {} times", count);
            // 达到RDB阈值；尝试开启子进程
            if(count >= insertCount){
                log.debug("RDBListener: insert count: {} - RDB Start!", count);
                if("LOW".equals(businessType)){
                    triggerRDBInLow();
                }
                else if("HIGH".equals(businessType)){
                    counter.reset();
                    // 采用 防抖机制(任务合并) 处理紧密连续的RDB任务(高频率下这些连续的RDB任务会影响性能并且大多数任务都不必要)...
                    handleHighFrequencyTrigger();
                }else {
                    throw new RuntimeException("Unsupported business type: " + businessType);
                }
                log.debug("Listener Reset! - {}", counter.longValue());
            }
        }, 0, intervalTime, TimeUnit.MILLISECONDS);
    }

    public static void addInsertCont(){
        counter.increment();
    }

    // 处理高频并发
    public static void handleHighFrequencyTrigger(){
        try {
            // 已经到了最大延迟次数；直接执行RDB
            if(delayCount.get() >= maxDelayCount){
                triggerRDBInHigh();
                log.info("达到了最大延迟次数!强制执行RDB");
                return;
            }

            // 取消存在的延时任务
            // 1. 因为该方法被调用就说明监视器发起了RDB请求；
            // 2. 如果还存在延时任务没有完成，说明这两次RDB请求是连续的，取消上一次请求(合并两次请求)
            if(currentTask != null && !currentTask.isCancelled()){
                boolean cancelled = currentTask.cancel(false);
                if(cancelled){
                    log.info("未执行的延时任务被取消~");
                }else {
                    log.info("未执行的延时任务取消失败!");
                }
            }

            // 提交新的延时任务；合并上一次紧邻请求(如果存在)，创建延时任务，延时过程中等待是否有紧邻的下一次该方法的调用
            // 如果存在紧邻的调用，当前延时任务会被取消
            currentTask = scheduleExecutor.schedule(() -> {
                try {
                    triggerRDBInHigh();
                }finally {
                    // 该延时任务被执行；进入下一次防抖处理；重置延时次数
                    log.info("(High business) RDB Task Start");
                    delayCount.set(0);
                }
            }, intervalTime, TimeUnit.MILLISECONDS);

            // 累加延时次数
            delayCount.incrementAndGet();
        }finally {

        }
    }

    // 高频场景触发RDB
    private static void triggerRDBInHigh(){
        rdbExecutor.submit(new Runnable() {
            @Override
            public void run() {
                log.debug("Task Submitted.");
                RDBPersistence.storage();
            }
        });
    }

    // 低频场景触发RDB；又监听器自行执行RDB逻辑，不需要再开启独立线程
    private static void triggerRDBInLow(){
        RDBPersistence.storage();
        counter.reset();
    }

    // 系统关闭时查看是否存在数据插入行为
    private static void forceExecuteRDB(){
        if(counter.longValue() > 0){
            triggerRDBInHigh();
        }
    }

    // 关闭用钩子方法
    private static void shutdownExecutor(ExecutorService executor){
        // 停止接受任务
        executor.shutdown();
        log.info("Exit!");
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                // 强制关闭任务
                List<Runnable> taskList = executor.shutdownNow();
                log.warn("Warn! 程序停止前存在未执行完成的RDB任务: {}", taskList);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
