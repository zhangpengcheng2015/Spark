package zhang.data_algorithms_book.chapter07;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FindAssociationRules {
    static JavaSparkContext createJavaSparkContext(){
        SparkConf conf = new SparkConf().setAppName("FindAssociationRules").setMaster("local");
        conf.setAppName("market-basket-analysis");
        //建立一个快速串行化器
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");

        conf.set("spark.kryoserializer.buffer.mb","32");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        return ctx;
    }

    //工具列表函数
    static List<String> toList(String transaction){
        String[] items = transaction.trim().split(",");
        List<String> list = new ArrayList<String>();
        for(String item:items){
            list.add(item);
        }
        return list;
    }

    //删除位置i的组合
    static List<String> removeOneItem(List<String> list,int i){
        if((list == null) || (list.isEmpty())) return list;
        if((i<0) || i > list.size()-1) return list;
        List<String> cloned = new ArrayList<String>(list);//克隆
        cloned.remove(i);
        return cloned;
    }

    public static void main(String[] args) {
//        if(args.length < 1){
//            System.err.println("Usage:FindAssociationRules<transactions>");
//            System.exit(1);
//        }

        String folderPath = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output";

        //清空文件夹
        Util.delFolder(folderPath);

        String transactionFileName = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Input\\transaction.txt";

        String resultFile1 =  "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result1";

        String resultFile2 = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result2";

        String resultFile3 = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result3";

        String resultFile4 = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result4";

        String resultFile5 = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result5";

        String resultFile6 = "D:\\Documents\\Develope\\Spark\\src\\main\\resources\\Output\\result6";
        //创建一个Spark上下文对象
        JavaSparkContext ctx = createJavaSparkContext();

        JavaRDD<String> transactions = ctx.textFile(transactionFileName,1);

        transactions.saveAsTextFile(resultFile1);

        //生成频繁模式

        final JavaPairRDD<List<String>,Integer> patterns =
                transactions.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
                    public Iterator<Tuple2<List<String>, Integer>> call(String transaction) throws Exception {
                        List<String> list = toList(transaction);
                        List<List<String>> combinations =
                                Combination.findSortedCombinations(list);
                        List<Tuple2<List<String>,Integer>> result =
                                new ArrayList<Tuple2<List<String>, Integer>>();
                        for(List<String> combList:combinations){
                            if(combList.size() > 0 ){
                                result.add(new Tuple2<List<String>, Integer>(combList,1));
                            }
                        }

                        return result.iterator();
                    }
                });

        patterns.saveAsTextFile(resultFile2);

        //组合/归约频繁模式
        JavaPairRDD<List<String>,Integer> combined = patterns.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                });

        combined.saveAsTextFile(resultFile3);

        //生成所有子模式
        final JavaPairRDD<List<String>,Tuple2<List<String>,Integer>> subpatterns =
                combined.flatMapToPair(new PairFlatMapFunction<
                        Tuple2<List<String>,Integer>,
                        List<String>,
                        Tuple2<List<String>,Integer>>(){

                    public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>>
                        call(Tuple2<List<String>, Integer> pattern) throws Exception {

                        List <Tuple2<List<String>,Tuple2<List<String>,Integer>>> result =
                                new ArrayList<Tuple2<List<String>, Tuple2<List<String>, Integer>>>();

                        List<String> list = pattern._1;
                        Integer frequency = pattern._2;

                        if (list.size() == 1) {
                            return result.iterator();
                        }

                        // pattern has more than one items
                        // result.add(new Tuple2(list, new Tuple2(null,size)));
                        for (int i=0; i < list.size(); i++) {
                            List<String> sublist = Util.removeOneItem(list, i);
                            result.add(new Tuple2(sublist, new Tuple2(list, frequency)));
                        }
                        return result.iterator();
                    }
                });
                subpatterns.saveAsTextFile(resultFile4);

        // STEP-6: combine sub-patterns
        JavaPairRDD<List<String>,Iterable<Tuple2<List<String>,Integer>>> rules = subpatterns.groupByKey();
        rules.saveAsTextFile(resultFile5);

        // STEP-7: generate association rules
        // Now, use (K=List<String>, V=Iterable<Tuple2<List<String>,Integer>>)
        // to generate association rules
        // JavaRDD<R> map(Function<T,R> f)
        // Return a new RDD by applying a function to all elements of this RDD.
        JavaRDD<List<Tuple3<List<String>,List<String>,Double>>> assocRules = rules.map(new Function<
                Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>>,
                List<Tuple3<List<String>,List<String>,Double>>>() {
            public List<Tuple3<List<String>,List<String>,Double>> call(Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) {
                List<Tuple3<List<String>,List<String>,Double>> result =
                        new ArrayList<Tuple3<List<String>,List<String>,Double>>();
                List<String> fromList = in._1;
                Iterable<Tuple2<List<String>,Integer>> to = in._2;
                List<Tuple2<List<String>,Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
                Tuple2<List<String>,Integer> fromCount = null;
                for (Tuple2<List<String>,Integer> t2 : to) {
                    // find the "count" object
                    if (t2._1 == null) {
                        fromCount = t2;
                    }
                    else {
                        toList.add(t2);
                    }
                }

                // Now, we have the required objects for generating association rules:
                //  "fromList", "fromCount", and "toList"
                if (toList.isEmpty()) {
                    // no output generated, but since Spark does not like null objects, we will fake a null object
                    return result; // an empty list
                }

                // now using 3 objects: "from", "fromCount", and "toList",
                // create association rules:
                for (Tuple2<List<String>,Integer>  t2 : toList) {
                    double confidence = (double) t2._2 / (double) fromCount._2;
                    List<String> t2List = new ArrayList<String>(t2._1);
                    t2List.removeAll(fromList);
                    result.add(new Tuple3(fromList, t2List, confidence));
                }
                return result;
            }
        });
        assocRules.saveAsTextFile(resultFile6);

        // done
        ctx.close();

        System.exit(0);
    }
}
