package tdm.accumulo;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import tdm.accumulo.Core.AccumuloCoreOperations;

public class App {

  public static void main(String[] args) {

    AccumuloClient client = Accumulo.newClient()
        .to("uno", "localhost:2181")
        .as("root", "secret")
        .build();

    AccumuloCoreOperations.createAccumuloTable(client);
    AccumuloCoreOperations.loadAccumuloTables(client);

    try (ScannerBase scan = client.createScanner("AirPlaneDelay",
        Authorizations.EMPTY)) {
      System.out.println("Gotham Police Department Persons of Interest:");
      for (Map.Entry<Key, Value> entry : scan) {
        System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(),
            entry.getValue());
      }
    } catch (TableNotFoundException e) {
      System.err.println(e.getMessage());
    }

  }
}
