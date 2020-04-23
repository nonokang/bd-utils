package org.bd.flink.idgenerate;

public class IdGeneratorBuilder {
    static public ESIdGenerator build(IdGeneratorType generatorType) {
        switch (generatorType) {
            case AUTO:
                return new AutoIdGenerator();
            case MD5:
                return MD5XdrIdGenerator.builder().build();
        }
        return null;
    }
}
