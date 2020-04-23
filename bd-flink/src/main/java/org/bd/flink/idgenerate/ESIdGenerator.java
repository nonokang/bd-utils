package org.bd.flink.idgenerate;

import java.io.Serializable;

public interface ESIdGenerator extends Serializable {
    String generate(final String msg, final int idLength);
}
