package tdm.accumulo.Core;

import java.nio.file.Paths;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

import tdm.accumulo.model.AirPlaneDelayInformationBean;
import tdm.accumulo.parser.CSVBean;

public class AccumuloCoreOperations {
    List<AirPlaneDelayInformationBean> results = null;
    public static final String TABLE_NAME = "AirPlaneDelay";

    public static void createAccumuloTable(AccumuloClient client) {
        try {
            if (client.tableOperations().exists(TABLE_NAME)) {
                client.tableOperations().delete(TABLE_NAME);
                client.tableOperations().create(TABLE_NAME);
            } else {
                client.tableOperations().create(TABLE_NAME);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void loadAccumuloTables(AccumuloClient client) {
        List<AirPlaneDelayInformationBean> results = null;

        try {
            results = CSVBean.beanBuilder(Paths.get("src/assets/csv/airplane_final.csv"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BatchWriter writer = client.createBatchWriter("AirPlaneDelay")) {

            for (int i = 0; i < results.size(); i++) {
                Mutation m = new Mutation(results.get(i).getID());
                /*
                 * @columnFamily => date
                 */
                m.put("date", "DAY_OF_MONTH", results.get(i).getDAY_OF_MONTH());
                m.put("date", "DAY_OF_WEEK", results.get(i).getDAY_OF_WEEK());

                /*
                 * @columnFamily => transporteur_information
                 */
                m.put("transporteur_information", "OP_UNIQUE_CARRIER", results.get(i).getOP_UNIQUE_CARRIER());
                m.put("transporteur_information", "OP_CARRIER_AIRLINE_ID", results.get(i).getOP_CARRIER_AIRLINE_ID());
                m.put("transporteur_information", "OP_CARRIER", results.get(i).getOP_CARRIER());
                m.put("transporteur_information", "TAIL_NUM ", results.get(i).getTAIL_NUM());
                m.put("transporteur_information", "OP_CARRIER_FL_NUM ", results.get(i).getOP_CARRIER_FL_NUM());

                /*
                 * @columnFamily => aeroport_origine
                 */
                m.put("aeroport_origine", "ORIGIN_AIRPORT_ID", results.get(i).getORIGIN_AIRPORT_ID());
                m.put("aeroport_origine", "ORIGIN_AIRPORT_SEQ_ID", results.get(i).getORIGIN_AIRPORT_SEQ_ID());
                m.put("aeroport_origine", "ORIGIN", results.get(i).getORIGIN());

                /*
                 * @columnFamily => aeroport_arrivee
                 */
                m.put("aeroport_arrivee", "DEST_AIRPORT_ID", results.get(i).getDEST_AIRPORT_ID());
                m.put("aeroport_arrivee", "DEST_AIRPORT_SEQ_ID", results.get(i).getDEST_AIRPORT_SEQ_ID());
                m.put("aeroport_arrivee", "DEST", results.get(i).getDEST());

                /*
                 * @columnFamily => depart_information
                 */
                m.put("depart_information", "DEP_TIME", results.get(i).getDEP_TIME());
                m.put("depart_information", "DEP_DEL15", results.get(i).getDEP_DEL15());
                m.put("depart_information", "DEP_TIME_BLK", results.get(i).getDEP_TIME_BLK());

                /*
                 * @columnFamily => arrivee_information
                 */
                m.put("arrivee_information", "ARR_TIME", results.get(i).getARR_TIME());
                m.put("arrivee_information", "ARR_DEL15", results.get(i).getARR_DEL15());

                /*
                 * @columnFamily => information_vol
                 */
                m.put("information_vol", "CANCELLED", results.get(i).getCANCELLED());
                m.put("information_vol", "DIVERTED", results.get(i).getDIVERTED());

                /*
                 * @columnFamily => distance_pieds
                 */
                m.put("distance_pieds", "DISTANCE", results.get(i).getDISTANCE());

                writer.addMutation(m);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            System.err.println(e.getMessage());
        }

    }
}
