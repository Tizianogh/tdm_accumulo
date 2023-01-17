package request;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.beust.jcommander.internal.Console;

import tdm.accumulo.Core.AccumuloCoreOperations;

public class Request {
    public static final String NAME = AccumuloCoreOperations.TABLE_NAME;

    /*
     * Nombre d'avion(s) annulé(s) dans le mois.
     */
    public static int getNumberOfCancelledInMonth(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());
        long startTime = System.currentTimeMillis();

        scanner.fetchColumn("information_vol", "CANCELLED");
        int numberCancelled = (int) scanner.stream()
                .filter($ -> $.getKey().getColumnQualifier().toString().equalsIgnoreCase("CANCELLED"))
                .map($ -> $.getValue())
                .filter($ -> $.toString().equals("1")).count();

        long endTime = System.currentTimeMillis();

        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
        scanner.close();
        return numberCancelled;
    }

    /*
     * Destination qui accueille le plus de vol
     */
    public static Map<Value, Long> getAeroportWithMostOccurences(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());

        long startTime = System.currentTimeMillis();
        scanner.fetchColumn("information_vol", "CANCELLED");

        scanner.fetchColumn("aeroport_arrivee", "DEST");
        Map<Value, Long> firstRestul = scanner.stream()
                .collect(Collectors.groupingBy($ -> $.getValue(), Collectors.counting()));
        Long maxValue = Collections.max(firstRestul.values());

        Map<Value, Long> result = new HashMap<>();
        for (Entry<Value, Long> entry : firstRestul.entrySet()) { // Iterate through HashMap
            if (entry.getValue() == maxValue) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        long endTime = System.currentTimeMillis();
        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
        scanner.close();
        return result;
    }

    public static int getDelayTimes(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());

        long startTime = System.currentTimeMillis();

        scanner.fetchColumn("arrivee_information", "ARR_DEL15");

        int countDelayInMonth = (((int) scanner.stream()
                .filter($ -> $.getKey().getColumnQualifier().toString().equalsIgnoreCase("ARR_DEL15"))
                .map($ -> $.getValue().toString()).filter($ -> $.equals("1.0")).count()) * 15) / 4;

        long endTime = System.currentTimeMillis();

        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");

        return countDelayInMonth;
    }
}
