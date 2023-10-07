package wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author changbp
 * @Date 2021-04-15 14:50
 * @Return
 * @Version 1.0
 */
public class DataSetWordCount {
    public static void main(String[] args) throws Exception {
        //创建有界数据流运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从指定文件路径加载数据
        String path = "D:\\MySpace\\StudayModule\\com.cbp.flink\\src\\main\\resources\\hello.txt";
        DataSet<String> data = env.readTextFile(path);
        //处理加载的数据
        DataSet<Tuple2<String, Integer>> resultSet = data
                //压平映射    自定义方法 和 lamada写法
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1);
        //lamada方式实现
//        DataSet<Tuple2<String, Integer>> resultSet = data
//                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
//                    String[] words = value.split(" ");
//                    for (String word : words) {
//                        out.collect(new Tuple2<>(word, 1));
//                    }
//                })
//                .groupBy(0)
//                .sum(1);
        //打印数据
        resultSet.print();
    }

    //自定义方法实现
    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
