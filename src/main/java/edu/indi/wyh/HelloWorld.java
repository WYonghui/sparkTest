package edu.indi.wyh;

import org.apache.log4j.Logger;

/*
* java -cp spark-test-1.0-SNAPSHOT.jar edu.indi.wyh.HelloWorld
* */

public class HelloWorld {

    private static Logger logger = Logger.getLogger(HelloWorld.class);

    public static void main(String[] args) {
        System.out.println("Hello world!");
        logger.info("LOG: Hello world!");
        String s = "Hello World !";
        for (String str: s.split("\\W+")){
            logger.info(str);
        }
    }
}
