package cn.cnic.bigdata.IO;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xjzhu@cnic.cn on 2017/4/13.
 */
public class KafkaTest {

    /**
     * Print contents to stdout.
     * @param <T> type of the input
     */
    private static class PrintFn<T> extends DoFn<T, T> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
        }
    }

    public interface KafkaHbaseOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("kafkaBroker")
        @Default.String("10.0.82.164:9092,10.0.82.166:9092,10.0.82.167:9092")
        String getKafkaBroker();
        void setKafkaBroker(String kafkaBroker);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("topic")
        @Default.String("beam")
        String getTopic();
        void setTopic(String topic);

    }


    public static void main(String [] args){

        KafkaHbaseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(KafkaHbaseOptions.class);
        Pipeline p = Pipeline.create(options);

        List<String> topics = ImmutableList.of(options.getTopic());


        p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBroker())
                .withTopics(topics)
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of())
                //.updateConsumerProperties(ImmutableMap.<String, Object>of("group.id", "beam-kakfa"))
                .withoutMetadata())
          .apply(Values.<String>create())
          .apply(ParDo.of(new PrintFn<String>()));


        p.run();

    }
}
