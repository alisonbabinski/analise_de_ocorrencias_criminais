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
public class Atividade7 {
        public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pratica");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> arquivo = sc.textFile("hdfs://localhost/Frameworks/spark/ocorrencias_criminais.csv");
        //JavaRDD<String> arquivo = sc.textFile("/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv");
        System.out.println("7 - Mes com a maior ocorrencia de crimes do tipo DECEPTIVE PRACTICE");
        System.out.println("--------------------------------");
        
        JavaRDD<String> filtrado = arquivo.filter(s-> {
        String[] campos = s.split(";");
        //Obtem os dados do campo Tipo
        String tipo = campos[4];
        //Verifica se ele eh DECEPTIVE PRACTICE
        if(tipo.equalsIgnoreCase("DECEPTIVE PRACTICE")){
            return true;
        }
            return false;
        });
        
        //Obtem o mes
        JavaRDD<Long> mesRDD = filtrado.map(s -> { 
            String[] campos = s.split(";");
            //converte para Long
            return Long.parseLong(campos[1]);
        });
        
        //Mapeia cada um dos meses separatamente
        Map<Long, Long> meses = mesRDD.countByValue();
        
        //Ordena de maneira decrescente
        Stream<Map.Entry<Long, Long>> stream =
            meses.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        
        Map<Long, Long> resultado = stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (e1,e2) -> e1, LinkedHashMap::new));
        
        System.out.println(resultado.toString());
    }
}
