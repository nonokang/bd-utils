package org.bd.flink;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class XDRRawMessage {
    private String  apn;                //1
    private String  cell_id;            //2
    private String  country;       //3
    private String  enterprise_name;    //4
    private String  timestamp_event;    //30
}
