package com.mycompany.atp_spark;

import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author alison.babinski
 */
public class Atividade2 {
    
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("pratica");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        
        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        //JavaRDD<String> arquivo = sc.textFile("/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv");
        System.out.println("2 - Quantidade de crimes por ano do tipo NARCOTICS");
        System.out.println("--------------------------------");
        
        JavaRDD<String> filtrado = arquivo.filter(s-> {
            String[] campos = s.split(";");
            //Obtem os dados do campo Tipo
            String tipo = campos[4];
            //Verifica se ele eh Narcotics
            if(tipo.equals("NARCOTICS"))
                return true;
            return false;
        });
        
        //Faz a separacao dos totais de cada ano      
        //JavaRDD<String> anoRDD = filtrado.map(s -> {
        //    String[] campos = s.split(";");
        //    String[] ano = campos[2]
        //    return ano;
        //});
 
        

        //Apresenta o resultado final
        System.out.println(arquivo.countByValue()); 

    }
    
    
    public static void apresentaResultado (Map<String, Long> genericRDD){

        for (Map.Entry<String, Long> entrada: genericRDD.entrySet()){
            System.out.println(entrada.getKey() + " = " + entrada.getValue());
            
        }
    }   
}
