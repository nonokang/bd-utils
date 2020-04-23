package org.bd.flink;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class XDREnrichedMessage implements Serializable {
    private String apn;
    private String cell_id;
    private String country;
    private String enterprise_name;
    private String timestamp_event;
}
