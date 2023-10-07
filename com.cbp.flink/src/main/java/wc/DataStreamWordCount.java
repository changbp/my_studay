package wc;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author changbp
 * @Date 2021-04-15 16:10
 * @Return
 * @Version 1.0
 */
public class DataStreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建无界数据流运行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数
        senv.setParallelism(4);

        //从文件路径加载数据
        String inputPath = "D:\\MySpace\\StudayModule\\com.cbp.flink\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = senv.readTextFile(inputPath);
        //flink自带参数工具
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String host = parameters.get("host");
        int port = parameters.getInt("port");

        //读取socket 流数据
//        DataStream<String> inputDataStream = senv.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream
                .flatMap(new DataSetWordCount.MyFlatMapFunction())
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.getField(0))
                .sum(1);
        resultStream.print();
        senv.execute();
    }
}
