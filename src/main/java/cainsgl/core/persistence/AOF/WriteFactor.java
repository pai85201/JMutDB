package cainsgl.core.persistence.AOF;

import cainsgl.core.excepiton.MutPersistenceException;
import cainsgl.core.persistence.AOF.impl.AOFListener;

/*
* 写入因子：决定写入线程的写入逻辑
* */
public class WriteFactor {

    // 指示set命令对应的写入线程将数据写入到哪一个缓冲区(主 or 从)
    private static int set_commandBufferMarker;
    // 指示list命运对应的写入线程将数据写入到哪一个缓冲区(主 or 从)
    private static volatile int list_commandBufferMarker;

    // 对外方法；获取当前写入因子；通过传入的命令来判断是哪一种命令类型
    public static int getFactor(String command){
        int commandType = determine(command);
        if(commandType == -1){
            throw new MutPersistenceException("Unknown command: " + command);
        }
        int writeFactor = doGetFactorByCommandType(commandType);
        if(writeFactor == -1){
            throw new MutPersistenceException("Unknown write factor: " + command);
        }
        return writeFactor;
    }

    // 对外方法；设置当前写入因子；通过传入的命令来判断是哪一种命令类型
    public static void setFactor(String command, int writeFactor){
        int commandType = determine(command);
        if(commandType == -1){
            throw new MutPersistenceException("Unknown command: " + command);
        }
        doSetFactorByCommandType(commandType, writeFactor);
    }

    // 根据命令判断是哪一种类型
    private static int determine(String command){
        if("set".equals(command)){
            return AOFListener.SET_BUFFER_POSITION;
        }
        else if("list".equals(command)){
            return AOFListener.LIST_BUFFER_POSITION;
        }
        return -1;
    }

    // 根据commandType获取factor
    private static int doGetFactorByCommandType(int commandType){
        if(AOFListener.SET_BUFFER_POSITION == commandType){
            return getSet_commandBufferMarker();
        }
        else if(AOFListener.LIST_BUFFER_POSITION == commandType){
            return getList_commandBufferMarker();
        }
        return -1;
    }

    // 根据commandType修改writeFactor
    private static void doSetFactorByCommandType(int commandType, int writeFactor){
        if(AOFListener.SET_BUFFER_POSITION == commandType){
            set_commandBufferMarker = writeFactor;
        }
        else if(AOFListener.LIST_BUFFER_POSITION == commandType){
            list_commandBufferMarker = writeFactor;
        }
    }

    private static int getSet_commandBufferMarker() {
        return set_commandBufferMarker;
    }
    private static int getList_commandBufferMarker(){
        return list_commandBufferMarker;
    }

}
