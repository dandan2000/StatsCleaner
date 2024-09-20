/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit4TestClass.java to edit this template
 */
package com.dandan.stats.cleaner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.ScanResult;
import redis.embedded.RedisServer;

/**
 *
 * @author dacelent
 */
public class StatsCleanerTest {

    public StatsCleanerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void calcFechaLimite() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        Integer days = 90;
        String fl = StatsCleaner.calcFechaLimite(days);
        System.out.println("Fecha limite: " + fl);

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, (days * -1));
        assertEquals("Date must be equals", sdf.format(calendar.getTime()), fl);

    }

    /**
     * Test of main method, of class StatsCleaner.
     *
     * La fecha yearMonthDayThreshold debe ser mayor la fecha de la clave debe
     * ser menor o igual
     */
    @Test
    public void myCleanStats() {

        Random random = new Random();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Long keyresult = 0L;

        try {
            RedisServer redisServer = new RedisServer(6379);
            redisServer.start();

            String host = "localhost";
            Jedis jedis = new Jedis(host, 6379);

            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, (-120));
            String datekey = sdf.format(calendar.getTime());

            jedis.set("stats/{service:10560}/cinstance:2ed39d13/metric:10680/hour:" + datekey + "13", ((Integer) random.nextInt(100)).toString());

            calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, (-100));
            datekey = sdf.format(calendar.getTime());

            jedis.set("stats/{service:10561}/cinstance:2ed39d13/metric:10681/hour:" + datekey, ((Integer) random.nextInt(100)).toString());

            calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, (-10));
            datekey = sdf.format(calendar.getTime());

            jedis.set("stats/{service:10562}/cinstance:2ed39d13/metric:10682/hour:" + datekey + "1518", ((Integer) random.nextInt(100)).toString());

            String cursor = "0";
            ScanResult<String> scanResult = jedis.scan(cursor);
            cursor = scanResult.getCursor();
            List<String> keys1 = scanResult.getResult();

            Iterator it = keys1.iterator();
            while (it.hasNext()) {
                String xkey = it.next().toString();
                System.out.println("key:" + xkey);
            }

            Integer days = 90;
            StatsCleaner.myCleanStats(jedis, days);

            cursor = "0";

            scanResult = jedis.scan(cursor);
            cursor = scanResult.getCursor();
            List<String> keys2 = scanResult.getResult();
            System.out.println("Number of keys:" + keys2.size());
            keyresult = Long.valueOf(keys2.size());

            Iterator iter = keys2.iterator();
            while (iter.hasNext()) {
                String xkey = iter.next().toString();
                System.out.println("key:" + xkey);
            }

            jedis.close();

            assertEquals("Should return only 1 key", keyresult, Long.valueOf(1));

            redisServer.stop();
        } catch (IOException ex) {
            Logger.getLogger(StatsCleanerTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
