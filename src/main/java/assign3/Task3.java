package assign3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;


public class Task3 {
    public static void main (String[] args) {
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        String threshold = args[2];

        SparkConf conf = new SparkConf();

        //read the Patient Data and GEO data with expression value data
        conf.setAppName("Comp5349 Assign3 Task3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> task2output = sc.textFile(inputDataPath);

        // support, genes(\t)
        JavaPairRDD<String, String> supGenesPair = task2output.mapToPair(
                line -> {
                    int pos = line.indexOf("\t");
                    String sup = line.substring(0, pos);
                    String genes = line.substring(pos+1);
                    return new Tuple2<>(sup, genes);
                }
        );

        JavaPairRDD<String, String> largeSet = supGenesPair.filter( line -> {
            if(line._2.indexOf("\t")>=0){
                return true;
            }else{
                return false;
            }
        });

        // support, genes(\t)
        Broadcast<List<Tuple2<String, String>>> broadcast = sc.broadcast(largeSet.collect());

        JavaPairRDD<Float, String> result = supGenesPair.flatMapToPair(line -> {
            List<Tuple2<String, String>> largeData = broadcast.value();

            List<Tuple2<Float, String>> list = new ArrayList<>();
            for(Tuple2<String, String> entry : largeData){

                Set<String> aSet = getSet(line._2);
                Set<String> bSet = getSet(entry._2());

                if(aSet.size() == bSet.size()){
                    continue;
                }

                if(bSet.containsAll(aSet)){
                    String r = line._2;
                    bSet.removeAll(aSet);
                    StringBuilder sb = new StringBuilder();
                    for(String s: bSet){
                        sb.append(s);
                        sb.append("\t");
                    }
                    String s_r = sb.toString();
                    s_r = s_r.substring(0, s_r.length()-1);

                    float support = Float.parseFloat(entry._1())/Float.parseFloat(line._1);
                    list.add(new Tuple2<>(support, r + "\t" + s_r));
                }

            }
            return list.iterator();
        });

        JavaRDD<String> finedResult = result.sortByKey(false, 1)
                .filter(line -> {
                    if(line._1 >= Float.parseFloat(threshold)){
                        return true;
                    }else{
                        return  false;
                    }
                })
                .map( line ->{
                    return line._2 + "\t" + line._1;
                });


        finedResult.saveAsTextFile(outputDataPath);
        sc.close();
    }

    private static Set<String> getSet(String line){
        String[] splits = line.split("\t");
        Set<String> set = new HashSet<>(Arrays.asList(splits));
        return set;
    }
}
