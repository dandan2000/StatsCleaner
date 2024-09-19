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
import java.time.Instant;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import redis.clients.jedis.params.ScanParams;

public class StatsCleaner {

    private static final int BATCHSIZE = 1000;
    private static final String MATCHPATTERN = "stats/.*/(month|week|day|hour|minute):(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})\\d*$";
    private static final String MATCHPATTERNSCAN = "stats/*";

    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java StatsCleaner <redis://:password@host:port/DB> <days>");
            System.out.println("Usage: java StatsCleaner redis://:pass1234@127.0.0.1:6379/2  90");
            System.out.println("The search pattern is "+ MATCHPATTERNSCAN);
            System.exit(1);
        }

        String password = System.getenv().get("sc_pass");

        if (password.isBlank() || password.isEmpty()) {
            System.out.println("No sc_pass found. Please set export sc_pass=redis_password");
            //System.exit(1);
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

    public static void myCleanStats(Jedis jedis, String dias) throws IOException {

        System.out.println("Total Keys on DB: " + jedis.dbSize());

        String fechaLimite = calcFechaLimite(dias);
        System.out.println("Threshold Date: " + fechaLimite);
        
        int deletedCounter = 0;


        try {
            String cursor = "0";
            do {
                ScanParams scanParams = new ScanParams().count(BATCHSIZE).match(MATCHPATTERNSCAN);
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                List<String> keys = scanResult.getResult();

                if(keys.isEmpty()){
                    continue;
                }else{
                    System.out.print(".");

                }

                Pipeline pipeline = jedis.pipelined();

                Iterator it = keys.iterator();
                while (it.hasNext()) {
                    String key = it.next().toString();
                    if (shouldDeleteKey(key, fechaLimite)) {
                        deletedCounter++;
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
        }finally {
            System.out.println("\n" + deletedCounter + " keys have been deleted.");
            jedis.close();
        }

    }

    /* Decide si la clave debe ser eliminada de acuerdo a si la fecha de la clave, es menor o igual a fechaLimite */
    public static boolean shouldDeleteKey(String key, String yearMonthDayThreshold) {
        Pattern RE = Pattern.compile(MATCHPATTERN);

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
