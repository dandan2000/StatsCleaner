/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit4TestClass.java to edit this template
 */
package com.dandan.stats.cleaner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
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
        String dias = "90";
        String fl = StatsCleaner.calcFechaLimite(dias);
        System.out.println("Fecha limite: " + fl);

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

        try {
            RedisServer redisServer = new RedisServer(6379);
            redisServer.start();

            String host = "localhost";
            Jedis jedis = new Jedis(host, 6379);

            String key = "stats/{service:10560}/cinstance:2ed39d13/metric:10680/hour:2023061513";
            System.out.println("La key:" + key);

            jedis.set("stats/{service:10560}/cinstance:2ed39d13/metric:10680/hour:2024091513", ((Integer) random.nextInt(100)).toString());
            jedis.set("stats/{service:10561}/cinstance:2ed39d13/metric:10681/hour:20240101", ((Integer) random.nextInt(100)).toString());
            jedis.set("stats/{service:10562}/cinstance:2ed39d13/metric:10682/hour:2024061518", ((Integer) random.nextInt(100)).toString());

            String cursor = "0";
            ScanResult<String> scanResult = jedis.scan(cursor);
            cursor = scanResult.getCursor();
            List<String> keys1 = scanResult.getResult();

            Iterator it = keys1.iterator();
            while (it.hasNext()) {
                String xkey = it.next().toString();
                System.out.println("key:" + xkey);
            }
            
           // jedis.get(key)


            //jedis = new Jedis(host, 6379);
            
            String dias = "90";            
            StatsCleaner.myCleanStats(jedis, dias);
            
            cursor = "0";

            scanResult = jedis.scan(cursor);
            cursor = scanResult.getCursor();
            List<String> keys2 = scanResult.getResult();
            System.out.println("Cant keys:" + keys2.size());

            Iterator iter = keys2.iterator();
            while (iter.hasNext()) {
                String xkey = iter.next().toString();
                System.out.println("key:" + xkey);
            }

            jedis.close();

            boolean result;

            //result = StatsCleaner.shouldDeleteKey(key, yearMonthDayThreshold);
//        if(!result){
//            System.out.println(":" + result + ":");
//            fail("Deberia se borrada.");            
//        }
            redisServer.stop();
        } catch (IOException ex) {
            Logger.getLogger(StatsCleanerTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
