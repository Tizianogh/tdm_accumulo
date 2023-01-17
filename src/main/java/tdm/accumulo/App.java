package tdm.accumulo;

import java.nio.file.Paths;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import request.Request;
import tdm.accumulo.Core.AccumuloCoreOperations;

public class App {
  public static final String NAME = AccumuloCoreOperations.TABLE_NAME;

  public static void main(String[] args) throws TableNotFoundException {
    System.out.println("test");

    AccumuloClient client = Accumulo.newClient()
        .to("uno", "localhost:2181")
        .as("root", "secret")
        .build();

    // AccumuloCoreOperations.createAccumuloTable(client);
    // AccumuloCoreOperations.loadAccumuloTables(client,
    // Paths.get("src/assets/csv/airplane_final.csv"));

    // Sur le mois, combien de vols cancelled ?
    // System.out.println(Request.getNumberOfCancelledInMonth(scanner));

    System.out.println(Request.getDelayTimes(client));
  }
}
