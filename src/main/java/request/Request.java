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

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Value;

import com.beust.jcommander.internal.Console;

public class Request {

    /*
     * Nombre d'avion(s) annulé(s) dans le mois.
     */
    public static int getNumberOfCancelledInMonth(Scanner scanner) {
        scanner.fetchColumn("information_vol", "CANCELLED");

        return (int) scanner.stream()
                .filter($ -> $.getKey().getColumnQualifier().toString().equalsIgnoreCase("CANCELLED"))
                .map($ -> $.getValue())
                .filter($ -> $.toString().equals("1")).count();
    }

    /*
     * Destination qui accueille le plus de vol
     */
    public static Map<Value, Long> getAeroportWithMostOccurences(Scanner scanner) {
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

        return result;
    }
}
