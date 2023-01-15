package tdm.accumulo.parser;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import tdm.accumulo.model.AirPlaneDelayInformationBean;

public class CSVBean {

    public static List<AirPlaneDelayInformationBean> beanBuilder(Path path)
            throws Exception {

        List<AirPlaneDelayInformationBean> results = null;
        try (Reader reader = Files.newBufferedReader(path)) {
            CsvToBean<AirPlaneDelayInformationBean> cb = new CsvToBeanBuilder<AirPlaneDelayInformationBean>(reader)
                    .withType(AirPlaneDelayInformationBean.class)
                    .build();

            results = cb.parse();
        }

        return results;
    }
}