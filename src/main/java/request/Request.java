package request;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
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
     * Les voyages les plus longs
     */
    public static Float getVoyageWithMaxDistance(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());
        long startTime = System.currentTimeMillis();
        scanner.fetchColumn("distance_pieds", "DISTANCE");

        List<String> listOfDistanceString = scanner.stream().map($ -> $.getValue().toString())
                .collect(Collectors.toList());

        List<Float> listOfDistanceInteger = listOfDistanceString.stream().map($ -> Float.valueOf($))
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();

        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");

        return listOfDistanceInteger.stream()
                .max(Comparator.comparing(Float::valueOf))
                .get();
    }

    /*
     * Nombre d'avion(s) annulé(s) dans le mois.
     */
    public static int getNumberOfCancelledInMonth(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());
        long startTime = System.currentTimeMillis();

        scanner.fetchColumn("information_vol", "CANCELLED");
        int numberCancelled = (int) scanner.stream()
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
        for (Entry<Value, Long> entry : firstRestul.entrySet()) {
            if (entry.getValue() == maxValue) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        long endTime = System.currentTimeMillis();
        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
        scanner.close();
        return result;
    }

    /*
     * Moyenne de delay dans le mois
     */
    public static int getDelayTimes(AccumuloClient client) throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());

        long startTime = System.currentTimeMillis();

        scanner.fetchColumn("arrivee_information", "ARR_DEL15");

        int countDelayInMonth = (((int) scanner.stream()
                .map($ -> $.getValue().toString()).filter($ -> $.equals("1.0")).count()) * 15) / 4;

        long endTime = System.currentTimeMillis();

        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");

        return countDelayInMonth;
    }

    /*
     * La moyenne de retard par jour de la semaine.
     */
    public static void getAvfOfDayOfWeekWithWithTheMostImportantDistance(AccumuloClient client)
            throws TableNotFoundException {
        Scanner scanner = client.createScanner(NAME, new Authorizations());
        long startTime = System.currentTimeMillis();

        scanner.fetchColumn("date", "DAY_OF_WEEK");
        scanner.fetchColumn("arrivee_information", "ARR_DEL15");

        scanner.stream()
                .collect(Collectors.groupingBy($ -> $.getKey().getColumnQualifier().toString(),
                        Collectors.counting()))
                .entrySet().forEach($ -> System.out.println($));

        // firstRestul.entrySet().forEach($ -> System.out.println($));

        long endTime = System.currentTimeMillis();

        System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
    }
}
