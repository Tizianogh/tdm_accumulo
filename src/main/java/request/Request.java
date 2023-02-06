package request;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.spi.DirStateFactory.Result;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
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
                System.out.println("Les voyages les plus longs : ");
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
                                .filter($ -> $.toString().equals("1.0")).count();

                long endTime = System.currentTimeMillis();
                System.out.println("Le nombre d'avions annulés dans le mois");
                System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
                scanner.close();
                return numberCancelled;
        }

        /*
         * Destination qui accueille le plus de vol
         */
        public static Map<Value, Long> getAeroportWithMostOccurences(AccumuloClient client)
                        throws TableNotFoundException {
                Scanner scanner = client.createScanner(NAME, new Authorizations());

                long startTime = System.currentTimeMillis();

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
                System.out.println("La destination qui accueille le plus de vol");
                System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
                scanner.close();
                return result;
        }

        /*
         * Moyenne de delay dans le mois
         */

        public static double getDelayTimes(AccumuloClient client) throws TableNotFoundException {
                Scanner scanner = client.createScanner(NAME, new Authorizations());

                long startTime = System.currentTimeMillis();

                scanner.fetchColumn("arrivee_information", "ARR_DEL15");

                int countDelayInMonth = (((int) scanner.stream()
                                .map($ -> $.getValue().toString()).filter($ -> $.equals("1.0")).count()) * 15) / 4;

                int countOfRetard = (((int) scanner.stream()
                                .map($ -> $.getValue().toString()).filter($ -> $.equals("1.0")).count()));

                int numberTot = (((int) scanner.stream()
                                .map($ -> $.getValue().toString()).count()));

                long endTime = System.currentTimeMillis();
                System.out.println("Le nombre d'heures de retard dans un mois : " + countDelayInMonth + " heures.");
                System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");

                return (double) countOfRetard / numberTot;
        }

        /*
         * Transporteur le plus sollicité dans le mois
         */
        public static Map<Value, Long> getTransporteurMostUse(AccumuloClient client) throws TableNotFoundException {
                Scanner scanner = client.createScanner(NAME, new Authorizations());

                long startTime = System.currentTimeMillis();
                scanner.fetchColumn("transporteur_information", "OP_CARRIER");

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
                System.out.println("Le transporteur le plus sollicité dans un mois");
                System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
                scanner.close();
                return result;
        }

        /*
         * La moyenne de retard par jour de la semaine.
         */
        public static Map<Object, Double> getAvfOfDayOfWeekWithWithTheMostImportantDistance(AccumuloClient client)
                        throws TableNotFoundException {
                Scanner scanner = client.createScanner(NAME, new Authorizations());
                long startTime = System.currentTimeMillis();

                scanner.fetchColumn("date", "DAY_OF_WEEK");
                scanner.fetchColumn("arrivee_information", "ARR_DEL15");

                int countOfRetard = (((int) scanner.stream()
                                .filter($ -> $.getKey().getColumnQualifier().toString().equals("ARR_DEL15"))
                                .map($ -> $.getValue().toString()).filter($ -> $.equals("1.0")).count()));

                int numberTot = (((int) scanner.stream()
                                .map($ -> $.getValue().toString()).count()));

                Map<Object, Long> firstRestul = scanner.stream()
                                .filter($ -> $.getKey().getColumnQualifier().toString().equals("DAY_OF_WEEK"))
                                .collect(Collectors.groupingBy($ -> $.getValue(), Collectors.counting()));

                Map<Object, Double> result = new HashMap<>();

                for (Entry<Object, Long> entry : firstRestul.entrySet()) {
                        result.put(entry.getKey(), (double) entry.getValue() / countOfRetard);

                }

                long endTime = System.currentTimeMillis();

                System.out.println("Moyenne de retards par jour de la semaine");
                System.err.println("Cette requête  a pris " + (endTime - startTime) + " milliseconds");
                return result;
        }

}