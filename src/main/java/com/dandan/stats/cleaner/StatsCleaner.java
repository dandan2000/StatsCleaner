package com.dandan.stats.cleaner;

/**
 *
 * @author dacelent
 */
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.params.ScanParams;

public class StatsCleaner {

    private static final int BATCHSIZE = 1000;
    private static final String MATCHPATTERN = "stats/.*/(month|week|day|hour|minute):(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})\\d*$";
    private static final String MATCHPATTERNSCAN = "stats/*";
    private static String masterPassword = "";
    private static String sentinelPassword = "";
    
    
    @SuppressWarnings("ConvertToTryWithResources")
    public static void main(String[] args) {
        if (args.length < 4) {
            // days host port db pass
            // redis://: + pass @ + host + : + port + / + db 
            // <redis://:pass_env@host:port/DB> <days>
            System.out.println("Usage: java StatsCleaner days sentinel_host:port,sentinel_host:port;... DB masterSetName masterPassword sentinelPassword");
            System.out.println("Usage: java StatsCleaner 90 127.0.0.1:26379,10.0.0.1:26379 2 mymaster pass1234 pass1111");
            System.out.println("For password setting also you can use export sc_pass=redis_password");
            System.out.println("If sc_pass is present it take precedence");
            System.out.println("The delete pattern is " + MATCHPATTERN);
            System.exit(1);
        }

        Integer days = Integer.valueOf(args[0]);

        String hosts = args[1];

        String db = args[2];

        String MASTER_NAME = args[3];

        Set<String> sentinels = new HashSet<>();
        String[] sentinelArray = hosts.split(",");
        sentinels.addAll(Arrays.asList(sentinelArray));

        if (args.length == 6) {
            masterPassword = args[4];
            sentinelPassword = args[5];            
        }

        String mast_pass_env = System.getenv().get("sc_master_pass");
        String sent_pass_env = System.getenv().get("sc_sentinel_pass");

        if (mast_pass_env != null && sent_pass_env != null  && !(mast_pass_env.isBlank() || mast_pass_env.isEmpty() || sent_pass_env.isBlank() || sent_pass_env.isEmpty())) {
            System.out.println("Using password from ENV sc_master_pass and sc_sentinel_pass");
            masterPassword = mast_pass_env;
            sentinelPassword = mast_pass_env;            
        }

        
        if (days < 1) {
            System.out.println("Days must be a natural number");
            System.exit(1);
        }

        // Crear el pool de Jedis con Sentinel... JedisSentinelPool elege el master para la conexion
        // password master, password sentinel en ese orden
        try (JedisSentinelPool sentinelPool = new JedisSentinelPool(MASTER_NAME, sentinels, masterPassword, sentinelPassword)) {

            try (Jedis jedis = sentinelPool.getResource()) {

                jedis.select(Integer.parseInt(db));

                myCleanStats(jedis, days);

            }
        } catch (Exception e) {
            System.out.println("Error Caused by: " + e.getMessage());
        }

    }

    public static void myCleanStats(Jedis jedis, Integer days) throws IOException {

        System.out.println("Total Keys on DB: " + jedis.dbSize());

        String fechaLimite = calcFechaLimite(days);
        System.out.println("Threshold Date: " + fechaLimite);

        int deletedCounter = 0;

        try {
            String cursor = "0";
            do {
                ScanParams scanParams = new ScanParams().count(BATCHSIZE).match(MATCHPATTERNSCAN);
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                List<String> keys = scanResult.getResult();

                if (keys.isEmpty()) {
                    continue;
                } else {
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
                Thread.sleep(20);

            } while (!cursor.equals("0"));

        } catch (Exception e) {
            System.out.println("\n*** ERROR: " + e.getMessage());
            System.out.print("\n*** ERROR: " + e.getMessage() + "\n");
        } finally {
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
    public static String calcFechaLimite(Integer dias) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, (dias * -1)); // se suman dias negativo.. se restan
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(calendar.getTime());
    }

}
