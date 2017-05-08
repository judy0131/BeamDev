package cn.cnic.bigdata.IO;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;
import java.lang.String;
import org.bson.Document;

import static org.apache.beam.sdk.io.mongodb.MongoDbIO.*;


/**
 * Created by xjzhu@cnic.cn on 2017/4/13.
 */
public class KafkaMongoDBTest {

    /**
     * Print contents to stdout.
     * @param <String> type of the input
     */
    private static class parseDocument<String> extends DoFn<String, Document> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
            String[] words = (String[]) c.element().toString().split(" ");
            String id = words[0];
            String name = words[1];
            String email = words[2];
            Document doc = Document.parse(java.lang.String.format("{\"id\":\"%s\",\"name\":\"%s\",\"email\":\"%s\"}",id,name,email));
            c.output(doc);
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


        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("uri")
        @Default.String("mongodb://10.0.82.172:27017")
        String getUri();
        void setUri(String uri);


        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("db")
        @Default.String("spark")
        String getDB();
        void setDB(String db);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("collection")
        @Default.String("beam")
        String getCollection();
        void setCollection(String collection);

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
          .apply(ParDo.of(new parseDocument<String>()))
          .apply(MongoDbIO.write()
                  .withUri(options.getUri())
                  .withDatabase(options.getDB())
                  .withCollection(options.getCollection()));

        p.run();

    }
}
