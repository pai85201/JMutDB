package cainsgl.core.persistence.AOF.impl;

import cainsgl.core.config.MutConfiguration;
import cainsgl.core.excepiton.MutPersistenceException;
import cainsgl.core.persistence.AOF.WriteFactor;
import cainsgl.core.persistence.AOF.valueObj.AOFReadVO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
* AOF_异步刷盘_实例方法
* */
public class AOFListener3 {
    private static final Logger log = LogManager.getLogger(AOFListener3.class);

    // 当前监听器负责的命令名称
    private final byte[] commandName;
    // 当前监听器实例观察的线程号
    private final long threadId;

    // 堆内存的分配大小
    private static final int bufferSize = MutConfiguration.AOF.BUFFER_SIZE;
    // 从缓冲区个数
    private static final int slaveCount = 1;
    // 堆内存缓冲区；用于缓冲命令信息；包括主从缓冲区结构
    private final ByteBuffer[] buffers;

    // AOF定时任务线程池；只负责向异步刷盘提交任务
    private final ScheduledExecutorService scheduler;
    // AOF间隔时间 单位/毫秒
    private static final long intervalTime = MutConfiguration.AOF.INTERVAL_TIME;

    // 异步刷盘线程
    private final ExecutorService diskExecutor;

    // 当前实例的原子类
    private final WriteFactor writeFactor;

    // AOF文件名称；暂时写死
    private static final String fileName = MutConfiguration.AOF.FILE_NAME;


    public AOFListener3(byte[] commandName, long threadId) {
        this.commandName = commandName;
        this.threadId = threadId;
        // 初始化该实例的原子类
        writeFactor = new WriteFactor();
        // 构建线程
        scheduler = Executors.newSingleThreadScheduledExecutor();
        diskExecutor = Executors.newSingleThreadExecutor();
        // 构建缓冲区
        buffers = new ByteBuffer[1 + slaveCount];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(bufferSize);
        }
        this.init();
    }


    private void init(){
        scheduler.scheduleAtFixedRate(() -> {
            log.info("AOF间隔时间达到, 被动触发AOF. 执行线程号 : {}", threadId);
            this.flushAsync();
        }, 0, intervalTime, TimeUnit.MILLISECONDS);
    }

    // 读取AOF文件
    public List<AOFReadVO> readAOF(){
        List<AOFReadVO> readVOs = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new FileInputStream(fileName))){
            while (dis.available() > 0) {
                // 先读取该命令的时间戳
                long commandTime = dis.readLong();
                // 在读取命令的长度以及命令本身
                int commandLength = dis.readInt();
                byte[] commandBytes = new byte[commandLength];
                dis.readFully(commandBytes);

                // 读取参数个数
                int argsCount = dis.readInt();
                byte[][] args = new byte[argsCount][];
                // 依次读取参数
                for (int i = 0; i < argsCount; i++) {
                    args[i] = new byte[dis.readInt()];
                    dis.readFully(args[i]);
                }

                // 构建值对象
                AOFReadVO readVO = new AOFReadVO(commandTime, commandBytes, args);
                readVOs.add(readVO);
            }
        }catch (IOException e){
            log.error("ERR While Read AOF", e);
        }
        // 从小到大排序
        Collections.sort(readVOs);
        return readVOs;
    }


    public void addCommand(byte[][] args, int writeFactor, long currentTime) {
        // 获取到当前用于写入的缓冲区；标识为 writeFactor
        ByteBuffer curBuffer = buffers[writeFactor];
        // 向缓冲区写入数据之前检查缓冲区空间是否充足
        long valueLength = 0L;
        for (byte[] arg : args) {
            valueLength += arg.length;
        }
        if((curBuffer.remaining()) < (commandName.length + valueLength)){
            // 达到阈值则触发异步刷盘
            log.info("缓冲区即将写满，触发AOF. 执行线程号: {}", threadId);
            this.flushAsync();
        }
        // 向缓冲区写入数据；先写入时间戳
        curBuffer.putLong(currentTime);
        // 再写入命令的长度以及命令本身
        curBuffer.putInt(commandName.length);
        curBuffer.put(commandName);
        // 再写入参数的个数
        curBuffer.putInt(args.length);
        // 依次写入参数
        for (byte[] arg : args) {
            curBuffer.putInt(arg.length);
            curBuffer.put(arg);
        }
    }

    private void flushAsync() {
        diskExecutor.submit(this::flush);
    }

    private void flush(){
        // 触发AOF，先切换原子类的factor属性
        writeFactor.setWriteFactor(1);
        // 拿到主缓冲区的引用
        ByteBuffer curBuffer = buffers[0];
        curBuffer.flip();
        // 执行刷盘逻辑
        try (FileChannel fileChannel = new FileOutputStream(fileName, true).getChannel()){
            int written = fileChannel.write(curBuffer);
            log.info("Written {} bytes to {}", written, curBuffer);
            // 清空缓冲区
            curBuffer.clear();
        }catch (IOException e){
            throw new MutPersistenceException("ERR While do AOF.");
        }
        // 刷盘完成；切换主从缓冲区并重置factor
        exchangeSlaveAndMaster();
        writeFactor.setWriteFactor(0);
    }

    // 切换指定缓冲区类型的主从模式
    private void exchangeSlaveAndMaster(){
        // 第一个元素永远是主缓冲区
        ByteBuffer temp = buffers[0];
        buffers[0] = buffers[1];
        buffers[1] = temp;
    }

    public class WriteFactor{
        private volatile int writeFactor;

        public void setWriteFactor(int writeFactor) {
            this.writeFactor = writeFactor;
        }

        public int getWriteFactor() {
            return writeFactor;
        }
    }

    public WriteFactor writeFactor(){
        return writeFactor;
    }

}
