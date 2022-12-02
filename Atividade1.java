package com.mycompany.atp_spark;

import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author alison.babinski
 */
public class Atividade1 {
    public static void main(String [] args){
        
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");
         JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        //JavaRDD<String> arquivo = sc.textFile("/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv");

        System.out.println("1 - Quantidade de crimes por ano");
        System.out.println("--------------------------------");
        
        JavaRDD<String> anoRDD = arquivo.map((String s) -> {
            //Faz a separacao dos campos baseadas no ;
            String [] campos = s.split(";");
            //Pega a segunda coluna, correspondente ao Ano
            String ano = campos[2];
            return ano;
        });
        System.out.println(anoRDD.countByValue());
    }
    
    public static void apresentaResultado (Map<String, Long> genericRDD){

        for (Map.Entry<String, Long> entrada: genericRDD.entrySet()){
            System.out.println(entrada.getKey() + " = " + entrada.getValue());
            
        }
    }   

}
