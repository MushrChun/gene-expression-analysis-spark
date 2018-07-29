package assign3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;


public class Task2 {
    public static void main (String[] args) {
        String inputDataPath = args[0];
        String outputDataPath = args[1];
        double minSup = Double.parseDouble(args[2]);
        int iterNum = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf();

        //read the Patient Data and GEO data with expression value data
        conf.setAppName("Comp5349 Assign3 Task2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> patientData = sc.textFile(inputDataPath+"PatientMetaData.txt");
        JavaRDD<String> GEOData = sc.textFile(inputDataPath + "GEO.txt");

        //transform the patient data to (patient id, disease)
        //then filter the disease which belongs to cancer
        JavaPairRDD<String, String> filterPatientData = patientData.flatMapToPair(line -> {

            String[] splits = line.split(",");

            ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>();

            if(!splits[0].equals("id")){
                String[] diseases = splits[4].split(" ");
                boolean flag = false;
                for(String disease : diseases){
                    if(("breast-cancer prostate-cancer pancreatic-cancer leukemia lymphoma".indexOf(disease) >= 0)){
                        flag = true;
                    }
                }
                if(flag){
                    results.add(new Tuple2<>(splits[0], "Cancer"));
                }

            }
            return results.iterator();
        });

        // calculate threshold based on rate
        double threshold = filterPatientData.count() * minSup;

        //filter the Gene whose expression value higher than 1250000
        //transform the data set as (Patient id, value)
        JavaPairRDD<String, String> filterGEOData = GEOData.filter(data -> {
            String[] splits = data.split(",");
            if (splits[0].equals("patientid")) {
                return false;
            }
            else {
                double expression_value = Double.parseDouble(splits[2]);
                if(expression_value > 1250000){
                    return true;
                }else{
                    return false;
                }
            }
        }).mapToPair(line -> {
            String[] splits = line.split(",");
            return new Tuple2<>(splits[0], splits[1]);
        });

        JavaPairRDD<String, Tuple2<String,String>> joinResults = filterPatientData.join(filterGEOData);

        //transformation geneID, patientList(split with" ")
        JavaPairRDD<String, String> genePatients = joinResults
                .mapToPair(line -> new Tuple2<>(line._2._2, line._1))
                .reduceByKey((a, b) -> a.replace("patient","") + " " + b.replace("patient",""));

        //get one tuple result : key gene , other gene, patiens, support
        JavaPairRDD<String, Tuple3<String, String, Integer>> initData = genePatients
                .mapToPair(line -> new Tuple2<>("0", new Tuple3<>(line._1, line._2, line._2.split(" ").length)))
                .filter( line -> {
                    if(line._2._3() >= threshold){
                        return true;
                    }else{
                        return false;
                    }
                });

        JavaPairRDD<Integer, String> initTotal = initData.mapToPair( line-> {
            int support = line._2._3();
            String genes = line._1 + "\t" + line._2._1();
            return new Tuple2<>(support, genes.substring(2));
        });

        Broadcast<List<Tuple2<String, Tuple3<String, String, Integer>>>> broadcast = sc.broadcast(initData.collect());


        for(int i =0 ; i < iterNum; i++){

            initData = initData
                    .flatMapToPair(new FrequentItemMapper(threshold, broadcast));

            broadcast = sc.broadcast(initData.collect());

            JavaPairRDD<Integer, String> tmpTotal = initData.mapToPair( line-> {
                int support = line._2._3();
                String genes = line._1 + "\t" + line._2._1();
                return new Tuple2<>(support , genes.substring(2));
            });

            initTotal = initTotal.union(tmpTotal);

        }


        initTotal = initTotal.sortByKey(false, 1);

        JavaRDD<String> styledData = initTotal.map( line -> line._1() + "\t" + line._2());

        styledData.saveAsTextFile(outputDataPath);
        sc.close();
    }
}
