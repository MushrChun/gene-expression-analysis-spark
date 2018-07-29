package assign3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;


public class Task1 {
    public static void main (String[] args) {
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        SparkConf conf = new SparkConf();

        //read the Patient Data and GEO data with expression value data
        conf.setAppName("Comp5349 Assign3 Task1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> patientData = sc.textFile(inputDataPath+"PatientMetaData.txt");
        JavaRDD<String> GEOData = sc.textFile(inputDataPath + "GEO.txt");

        //transform the patient data to (patient id, disease)
        //then filter the disease which belongs to cancer
        JavaPairRDD<String, String> filterPatientData  = patientData.flatMapToPair(line -> {

            String[] splits = line.split(",");

            ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>();

            if(!splits[0].equals("id")){
                String[] diseases = splits[4].split(" ");
                for(String disease : diseases){
                    if(("breast-cancer prostate-cancer pancreatic-cancer leukemia lymphoma".indexOf(disease) >= 0)){
                        results.add(new Tuple2<>(splits[0], disease));
                    }
                }

            }
            return results.iterator();
        });

        //filter the Gene which id equals 42 and expression value higher than 1250000
        //transform the data set as (Patient id, value)
        JavaPairRDD<String, Integer> filterGEOData = GEOData.filter(data -> {
            String[] splits = data.split(",");
            if (splits[0].equals("patientid")) {
                return false;
            } else if (splits[1].equals("42")){
                double expression_value = Double.parseDouble(splits[2]);
                if(expression_value > 1250000){
                    return true;
                }else{
                    return false;
                }
            } else {
                return false;
            }
        }).mapToPair(line -> {
            String[] splits = line.split(",");
            return new Tuple2<>(splits[0], 1);
        });

        JavaPairRDD<String, Tuple2<String,Integer>> joinResults = filterPatientData.join(filterGEOData);
        JavaPairRDD<String, Integer> joinResultsByCancer = joinResults.mapToPair(line -> new Tuple2<>(line._2._1, line._2._2)
        );

        JavaPairRDD<String, Integer> stat = joinResultsByCancer.reduceByKey((a, b) -> a + b)
                .sortByKey(true, 1)
                .mapToPair(line -> new Tuple2<>(line._2(), line._1()))
                .sortByKey(false, 1)
                .mapToPair(line -> new Tuple2<>(line._2(), line._1()))
                ;

        JavaRDD<String> finalStat = stat.map(line -> line._1() +"\t"+ line._2());

        finalStat.saveAsTextFile(outputDataPath);
        sc.close();
    }
}
