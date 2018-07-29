package assign3P;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 * Created by MushrChun on 9/6/17.
 */
public class FrequentItemMapper implements PairFlatMapFunction<Tuple2<String, Tuple2<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>>, String, Tuple3<String, String, Integer>> {

    private double threshold;

    public FrequentItemMapper(double threshold){
        this.threshold = threshold;
    }

    @Override
    public Iterator<Tuple2<String, Tuple3<String, String, Integer>>> call(Tuple2<String, Tuple2<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>> line) throws Exception {
        List<String> totalList = new ArrayList<>();
        List<Tuple2<String, Tuple3<String, String, Integer>>> l = new ArrayList<>();

        Tuple3<String, String, Integer> aValue = line._2._1;
        Tuple3<String, String, Integer> bValue = line._2._2;


        //remove 0,1 with 0,1
        int aNum = Integer.parseInt(aValue._1());
        int bNum = Integer.parseInt(bValue._1());
        if(aNum >= bNum){
            return l.iterator();
        }


        Set<String> a = new HashSet<>();
        String[] a_splits = aValue._2().split(" ");
        for(String i : a_splits){
            a.add(i);
        }

        Set<String> b = new HashSet<>();
        String[] b_splits = bValue._2().split(" ");
        for(String i : b_splits){
            b.add(i);
        }

        a.retainAll(b);

        StringBuffer newPatients = new StringBuffer();
        for(String i : a){
            newPatients.append(i);
            newPatients.append(" ");
        }

        int support = a.size();

        if(support >= threshold){
            String preKey = line._1;
            l.add(new Tuple2<String, Tuple3<String, String, Integer>>(preKey +"\t"+aValue._1(), new Tuple3<String, String, Integer> (bValue._1(), newPatients.toString().trim(), support)));
        }

        return l.iterator();

    }
}
