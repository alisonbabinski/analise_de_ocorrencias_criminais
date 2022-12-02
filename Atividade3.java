package com.mycompany.atp_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author alison.babinski
 */
public class Atividade3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        //JavaRDD<String> arquivo = sc.textFile("/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv");
        System.out.println("3 - Quantidade de crimes por ano que sejam do tipo NARCOTICS e tenham ocorrido em dias pares");
        System.out.println("--------------------------------");

        JavaRDD<String> filtrado;
        filtrado = arquivo.filter(s ->{
            String[] campos = s.split(";");
            String tipo = campos[4];
            //Converte o campo Dia para tipo inteiro para o calculo
            int dia = Integer.parseInt(campos[0]);
            //Faz a verificacao de Narcotics e se eh par simultaneamente
            if(tipo.equalsIgnoreCase("NARCOTICS") && (dia % 2 ==0)) {
                return true;
            } else {
                return false;
            }
        });
        
        JavaRDD<String> anoRDD = filtrado.map(s -> {
            String[] campos = s.split(";");
            return campos[2];
        });

         System.out.println(anoRDD.countByValue()); 

    }
}
