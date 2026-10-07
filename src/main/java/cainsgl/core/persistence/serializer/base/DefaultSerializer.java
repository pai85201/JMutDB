package cainsgl.core.persistence.serializer.base;

import cainsgl.core.persistence.serializer.MutSerializable;

import cainsgl.core.persistence.serializer.converter.byteConverter.ByteConverterRegistry;
import cainsgl.core.persistence.serializer.util.SeriUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;

/*
* 默认序列化器
* 1. 只负责将Map<byte[], byte[]>序列化为byte数组
* 2. 建造者模式；避免后期扩展参数
* 3. 通过直接操作字节数组，减少使用相关API时方法调用产生的包括对象创建以及动态扩容等开销
* */
public class DefaultSerializer {
    private static final Logger log = LoggerFactory.getLogger(DefaultSerializer.class);

    private String keyClassName;
    private String valueClassName;

    private final Map<byte[], byte[]> map;

    private static final int INT_LENGTH_PREFIX = 4;

    private DefaultSerializer(Map<byte[], byte[]> map) {
        this.map = map;
    }

    // 对外方法；实现为实例方法；需要将类名信息写入
    public static InitialBuilder defaultSerialize(Map<byte[], byte[]> map) {
        return new InitialBuilder(map);
    }

    // 对外方法；简单序列化方法；不记录类型数据
    public static byte[] simpleSerialize(Map<byte[], byte[]> map) {
        return doSimpleSerialize(map);
    }

    // 对外方法；反序列化字节数组
    public static void deserialize(byte[] mapInfo, Map<?, ?> map) {
        doDeserialize(mapInfo, map);
    }

    // 反序列化方法
    private static <K, V> void doDeserialize(byte[] mapInfo, Map<K, V> map){

        // 先读取类信息
        int offset = 0;
        String keyClassName;
        String valueClassName;

        // 读取keyClassName的长度
        byte[] keyClassNameLengthInfo = new byte[INT_LENGTH_PREFIX];
        System.arraycopy(mapInfo, offset, keyClassNameLengthInfo, 0, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;

        // 根据 keyClassNameLengthInfo 读取 keyClassName 的具体类容
        int keyClassLength = SeriUtil.bytesToInt(keyClassNameLengthInfo);
        byte[] keyClassNameBytes = new byte[keyClassLength];
        System.arraycopy(mapInfo, offset, keyClassNameBytes, 0, keyClassLength);
        offset += keyClassLength;
        keyClassName = new String(keyClassNameBytes);

        // 读取valueClassName的长度
        byte[] valueClassNameLengthInfo = new byte[INT_LENGTH_PREFIX];
        System.arraycopy(mapInfo, offset, valueClassNameLengthInfo, 0, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;

        // 根据 valueClassNameLengthInfo 读取 valueClassName 的具体内容
        int valueClassLength = SeriUtil.bytesToInt(valueClassNameLengthInfo);
        byte[] valueClassNameBytes = new byte[valueClassLength];
        System.arraycopy(mapInfo, offset, valueClassNameBytes, 0, valueClassLength);
        offset += valueClassLength;
        valueClassName = new String(valueClassNameBytes);

        // 先读取entry的个数
        byte[] entryCountInfo = new byte[INT_LENGTH_PREFIX];
        System.arraycopy(mapInfo, offset, entryCountInfo, 0, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;
        int entryCount = SeriUtil.bytesToInt(entryCountInfo);


        log.info("keyClass: {}; valueClass: {}", keyClassName, valueClassName);
        // 依次将entry读取出来
        for (int i = 0; i < entryCount; i++) {
            // 1. 先将key的长度读取出来
            byte[] keyInfo = new byte[INT_LENGTH_PREFIX];
            System.arraycopy(mapInfo, offset, keyInfo, 0, INT_LENGTH_PREFIX);
            offset += INT_LENGTH_PREFIX;
            int keyLength = SeriUtil.bytesToInt(keyInfo);
            // 根据key的长度读取key的字节信息
            byte[] key = new byte[keyLength];
            System.arraycopy(mapInfo, offset, key, 0, keyLength);
            offset += keyLength;

            // 2. 将value的长度读取出来
            byte[] valueInfo = new byte[INT_LENGTH_PREFIX];
            System.arraycopy(mapInfo, offset, valueInfo, 0, INT_LENGTH_PREFIX);
            offset += INT_LENGTH_PREFIX;
            int valueLength = SeriUtil.bytesToInt(valueInfo);
            // 根据value的长度读取字节信息
            byte[] value = new byte[valueLength];
            System.arraycopy(mapInfo, offset, value, 0, valueLength);
            offset += valueLength;

            // 将 keyBytes，valueBytes转化为对应的实现类
            try {
                // 拿到key的实例对象
                Class<?> keyClazz = Class.forName(keyClassName);
                // TODO 向对象填充数据 - 解耦
                K keyObj = (K)ByteConverterRegistry.getConverter(keyClazz).deConverter(key);

                // 拿到value的实例对象
                Class<?> valueClazz = Class.forName(valueClassName);
                // TODO 向对象填充数据 - 解耦
                V valueObj = (V)ByteConverterRegistry.getConverter(valueClazz).deConverter(value);

                map.put(keyObj, valueObj);
            }catch (Exception exception){
                log.warn("Class not found while deserializing map info", exception);
            }
        }

    }

    // 简单序列化方法；不记录类型
    private static byte[] doSimpleSerialize(Map<byte[], byte[]> map) {
        // 预计算产生的字节数组的总长度；避免过程中的动态分配产生的性能与内存开销
        int totalLength = 0;
        // 先将存储entry数量的count的字节长度存入
        totalLength += INT_LENGTH_PREFIX;
        int offset = 0;
        int entryCount = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            // 长度前缀法；key，和value的字节长度先暂定为 4个字节
            totalLength += INT_LENGTH_PREFIX + entry.getKey().length;
            totalLength += INT_LENGTH_PREFIX + entry.getValue().length;
            entryCount++;
        }

        // 结果 result
        byte[] res = new byte[totalLength];

        // 先将存储entry数量的count写入res
        System.arraycopy(SeriUtil.intToBytes(entryCount), offset, res, 0, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;

        // 直接操作字节数组；得到结果
        copyMapEntriesToRes(map, res, offset);

        // 返回结果
        return res;
    }

    // 默认序列化方法；需要将类名信息存储在Byte数组中
    public byte[] doDefaultSerialize() {
        // 先获取总长度
        int totalLength = 0;
        int offset = 0;
        int entryCount = 0;
        // 先计算类名信息占用的字节数；以及用于存储类名信息的长度前缀
        totalLength += INT_LENGTH_PREFIX * 2;
        totalLength += keyClassName.getBytes().length;
        totalLength += valueClassName.getBytes().length;
        // 再把记录entry数量的字节长度加入totalLength
        totalLength += INT_LENGTH_PREFIX;
        // 再计算每一个entry占用的字节长度
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            totalLength += INT_LENGTH_PREFIX + entry.getKey().length;
            totalLength += INT_LENGTH_PREFIX + entry.getValue().length;
            entryCount++;
        }

        byte[] res = new byte[totalLength];

        // 先将keyClassName的长度存入字节数组；占位 INT_LENGTH_PREFIX
        System.arraycopy(SeriUtil.intToBytes(keyClassName.length()), 0, res, offset, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;
        // 再将keyClassName的具体信息存入字节数组
        System.arraycopy(keyClassName.getBytes(), 0, res, offset, keyClassName.getBytes().length);
        offset += keyClassName.getBytes().length;

        // 同上
        System.arraycopy(SeriUtil.intToBytes(valueClassName.length()), 0, res, offset, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;
        System.arraycopy(valueClassName.getBytes(), 0, res, offset, valueClassName.getBytes().length);
        offset += valueClassName.getBytes().length;

        // 先将存储entry数量的count写入res
        System.arraycopy(SeriUtil.intToBytes(entryCount), 0, res, offset, INT_LENGTH_PREFIX);
        offset += INT_LENGTH_PREFIX;

        // 再依次将每一个entry填入字节数组
        copyMapEntriesToRes(map, res, offset);

        return res;
    }


//-----                          建造者模式设置参数                              -----//
    // 初始建造者：仅允许设置类名
    public static class InitialBuilder {
        private final Map<byte[], byte[]> map;

        InitialBuilder(Map<byte[], byte[]> map) {
            this.map = map;
        }

        public FinalBuilder setClassName(String keyClassName, String valueClassName) {
            return new FinalBuilder(map, keyClassName, valueClassName);
        }
    }

    // 最终建造者：允许执行序列化
    public static class FinalBuilder {
        private final Map<byte[], byte[]> map;
        private final String keyClassName;
        private final String valueClassName;

        FinalBuilder(Map<byte[], byte[]> map, String keyClassName, String valueClassName) {
            this.map = map;
            this.keyClassName = keyClassName;
            this.valueClassName = valueClassName;
        }

        public byte[] doDefaultSerialize() {
            return new DefaultSerializer(map, keyClassName, valueClassName).doDefaultSerialize();
        }
    }

    private DefaultSerializer(
            Map<byte[], byte[]> map,
            String keyClassName,
            String valueClassName
    ) {
        this.map = map;
        this.keyClassName = keyClassName;
        this.valueClassName = valueClassName;
    }


//-----                           基础方法                                        -----//
    private static void copyMapEntriesToRes(Map<byte[], byte[]> map, byte[] res, int offset) {
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            // 填入 key的长度
            System.arraycopy(SeriUtil.intToBytes(entry.getKey().length), 0, res, offset, INT_LENGTH_PREFIX);
            offset += INT_LENGTH_PREFIX;
            // 再写入key的值
            System.arraycopy(entry.getKey(), 0, res, offset, entry.getKey().length);
            offset += entry.getKey().length;

            // value同理
            System.arraycopy(SeriUtil.intToBytes(entry.getValue().length), 0, res, offset, INT_LENGTH_PREFIX);
            offset += INT_LENGTH_PREFIX;
            System.arraycopy(entry.getValue(), 0, res, offset, entry.getValue().length);
            offset += entry.getValue().length;
        }
    }
}