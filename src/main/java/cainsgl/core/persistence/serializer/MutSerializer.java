package cainsgl.core.persistence.serializer;

import cainsgl.core.persistence.serializer.base.DefaultSerializer;
import cainsgl.core.persistence.serializer.converter.SimpleMapConverter;
import cainsgl.core.persistence.serializer.converter.SuperConverter;
import cainsgl.core.persistence.serializer.valueObj.ConverterVO;

import java.util.Map;
import java.util.Objects;

/*
* 用户方调用中间人
* */
public class MutSerializer {

    // 对外方法，可扩展策略模式；序列化为字节数组
    public static byte[] serialize(Map<?, ?> map){
        return doSerializeDefault(map);
    }

    // 对外方法，可扩展策略模式；反序列化字节数组；并将反序列的数据填入 targetMap
    public static void deserialize(byte[] data, Map<?, ?> targetMap){
        doDeserialize(data, targetMap);
    }

    private static byte[] doSerializeDefault(Map<?, ?> map){
        // 1. 用户将Map传递给中间人；中间人调用转化器将map转化为默认类型
        ConverterVO converterVO = SuperConverter.defaultConverter(map);
        Map<byte[], byte[]> defaultMap = converterVO.getValues();

        // 2. 中间人不具备序列化任何类型的能力；调用默认序列化器将转化后的默认类型序列化为byte数组
        byte[] mapInfo = DefaultSerializer.defaultSerialize(defaultMap)
                .setClassName(converterVO.getKeyType(), converterVO.getValueType())
                .doDefaultSerialize();

        // 3. 返回byte数组
        return mapInfo;
    }

    private static void doDeserialize(byte[] mapInfo, Map<?, ?> targetMao){
        DefaultSerializer.deserialize(mapInfo, targetMao);
    }

}
