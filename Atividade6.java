package com.mycompany.atp_spark;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author alison.babinski
 */
public class Atividade6 {
        public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        //JavaRDD<String> arquivo = sc.textFile("/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv");
        System.out.println("6 - Mes por ano com a maior ocorrencia de crimes");
        System.out.println("--------------------------------");
        
        JavaRDD<String> mesRDD = arquivo.map(s -> {
            String[] campos = s.split(";");
            //Pega o campo mes
            String mes = campos[1];
            //Pega o campo ano
            String ano = campos[2];
            //junta ano e mes em um unico campo
            return String.join("/", ano, mes);
        });

        Map<String, Long> meses = mesRDD.countByValue();
        //Faz a ordendacao descrescente dos meses
        Stream<Map.Entry<String, Long>> stream =
                meses.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        Map<String, Long> resultado = stream.collect(Collectors.toMap(Map.Entry::getKey, 
                Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        
        
        for (Map.Entry<String, Long> entrada : resultado.entrySet()) {
            //Obtem o mes/ano
            String chave = entrada.getKey();
            //obtem o valor do periodo
            Long valor = entrada.getValue();
            //calcula a media por mes e apresenta
            System.out.println(String.format("Mes/Ano: %s - Qtde: %d", chave, valor)); 
        }


    }

}
