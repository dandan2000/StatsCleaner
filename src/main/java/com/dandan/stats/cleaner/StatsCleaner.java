package com.dandan.stats.cleaner;

/**
 *
 * @author dacelent
 */
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.ScanResult;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Instant;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import redis.clients.jedis.params.ScanParams;

public class StatsCleaner {

    private static final int BATCHSIZE = 1000;
    private static final String matchPattern = "stats/.*/(month|week|day|hour|minute):(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})\\d*$";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java StatsCleaner <redis://host:port/DB> <days>");
            System.out.println("Usage: java StatsCleaner redis://127.0.0.1:6379/2  90");
            System.out.println("The search pattern is stats/*/*");
            System.exit(1);
        }

        String host = args[0];
        String days = args[1];
        Jedis jedis = null;

        try {
            jedis = new Jedis(host);
            myCleanStats(jedis, days);
        } catch (IOException e) {
            System.out.println("Error... " + e.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close();
            }

        }
    }

    private static void cleanStats(String host, String matchPattern, String resumeAt) throws IOException {
        Jedis jedis = new Jedis(host);
        //jedis.select(2);

        int dels = 0;
        int count = 0;
        Instant now = Instant.now();

        String logFilename = "StatsCleaner_" + now.getEpochSecond() + ".log";
        FileWriter out = new FileWriter(logFilename);

        out.write("Logged on " + now + "\n\nRunning on " + host + " with " + matchPattern
                + " and batchsize " + BATCHSIZE + " resuming at " + (resumeAt != null ? resumeAt : "0") + "\n\nINFO:\n");

        // Escribir información de Redis en el archivo log
        jedis.info().lines().forEach(line -> {
            try {
                out.write(line + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        out.write("\nOUTPUT:\n");
        out.flush();

        try {
            String cursor = "0";
            do {
                ScanResult<String> scanResult = jedis.scan(cursor);
                cursor = scanResult.getCursor();
                List<String> keys = scanResult.getResult();

                if (!keys.isEmpty()) {
                    System.out.print(".");
                    Pipeline pipeline = jedis.pipelined();

                    // Filtrar las claves por fecha
                    keys.removeIf(key -> !shouldDeleteKey(key, resumeAt));

                    System.out.println("Cant keys:" + keys.size());

                    if (!keys.isEmpty()) {
                        System.out.print("S");
                        List<String> values = jedis.mget(keys.toArray(new String[0]));

                        // Escribir las claves y valores en el archivo log
                        for (int i = 0; i < keys.size(); i++) {
                            out.write(keys.get(i) + " => " + values.get(i) + "\n");
                        }
                        out.flush();

                        System.out.print("M");
                        // Eliminar las claves
                        pipeline.del(keys.toArray(new String[0]));
                        dels += keys.size();
                        System.out.print("D" + keys.size());

                        pipeline.sync();
                        Thread.sleep(10);
                    }
                }
            } while (!cursor.equals("0"));
        } catch (InterruptedException e) {
            System.out.println("\n*** INTERRUPTED\n");
            out.write("\n*** INTERRUPTED\n");
        } catch (Exception e) {
            System.out.println("\n*** ERROR: " + e.getMessage());
            out.write("\n*** ERROR: " + e.getMessage() + "\n");
        } finally {
            // Registro final de la operación
            out.write("\nDELS: " + dels + "\n\nFINAL INFO:\n");
            jedis.info().lines().forEach(line -> {
                try {
                    out.write(line + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            out.flush();
            out.close();
            jedis.close();
        }
    }

    public static void myCleanStats(Jedis jedis, String dias) throws IOException {

        System.out.println("DBsize: " + jedis.dbSize());

        String fechaLimite = calcFechaLimite(dias);
        System.out.println("Fecha Limite: " + fechaLimite);

        try {
            String cursor = "0";
            do {
                ScanParams scanParams = new ScanParams().count(BATCHSIZE).match(matchPattern);
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                List<String> keys = scanResult.getResult();
                
                while (keys.isEmpty()) {
                    cursor = scanResult.getCursor();
                    keys = scanResult.getResult();

                }

                System.out.print("." + keys.size());

                Pipeline pipeline = jedis.pipelined();

                Iterator it = keys.iterator();
                while (it.hasNext()) {
                    String key = it.next().toString();
                    if (shouldDeleteKey(key, fechaLimite)) {
                        pipeline.del(key);
                        System.out.println("Deleting: " + key);

                    }
                }

                pipeline.sync();
                Thread.sleep(10);

            } while (!cursor.equals("0"));

        } catch (Exception e) {
            System.out.println("\n*** ERROR: " + e.getMessage());
            System.out.print("\n*** ERROR: " + e.getMessage() + "\n");
        }
    }

    /* Decide si la clave debe ser eliminada de acuerdo a si la fecha de la clave, es menor o igual a fechaLimite */
    public static boolean shouldDeleteKey(String key, String yearMonthDayThreshold) {
        Pattern RE = Pattern.compile(matchPattern);

        //System.out.println("claves: " + key);
        //System.out.println("fechas: " + yearMonthDayThreshold);
        boolean result = false;
        Matcher matcher = RE.matcher(key);
        if (matcher.matches()) {
            String dateStr = matcher.group("year") + matcher.group("month") + matcher.group("day");
            result = Integer.parseInt(dateStr) <= Integer.parseInt(yearMonthDayThreshold);
        }
        //System.out.println("resultado: " + result);

        return result;
    }

    /* No se limita desde la fecha actual hasta la fecha limite, anterior a esta se eliminara */
    public static String calcFechaLimite(String dias) {
        Calendar calendar = Calendar.getInstance();
        Integer numDias = Integer.parseInt(dias) * -1;
        calendar.add(Calendar.DATE, numDias); // se suman dias negativo.. se restan
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(calendar.getTime());
    }

}
