package tdm.accumulo.model;

import com.opencsv.bean.CsvBindByPosition;

import lombok.Data;

@Data
public class AirPlaneDelayInformationBean {
    private String ID;
    private String DAY_OF_MONTH;
    private String DAY_OF_WEEK;
    private String OP_UNIQUE_CARRIER;
    private String OP_CARRIER_AIRLINE_ID;
    private String OP_CARRIER;
    private String TAIL_NUM;
    private String OP_CARRIER_FL_NUM;
    private String ORIGIN_AIRPORT_ID;
    private String ORIGIN_AIRPORT_SEQ_ID;
    private String ORIGIN;
    private String DEST_AIRPORT_ID;
    private String DEST_AIRPORT_SEQ_ID;
    private String DEST;
    private String DEP_TIME;
    private String DEP_DEL15;
    private String DEP_TIME_BLK;
    private String ARR_TIME;
    private String ARR_DEL15;
    private String CANCELLED;
    private String DIVERTED;
    private String DISTANCE;
}
