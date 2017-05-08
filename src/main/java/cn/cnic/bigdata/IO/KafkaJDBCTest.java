package cn.cnic.bigdata.IO;

import com.google.cloud.sql.jdbc.PreparedStatement;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
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
public class KafkaJDBCTest {

    /**
     * Print contents to stdout.
     * @param <T> type of the input
     */
    private static class PrintFn<T> extends DoFn<T, T> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
            c.output(c.element());
        }
    }

    public static class ExtractFn extends DoFn<String, KV<ByteString, Iterable<Mutation>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(" ");
            String key = words[0];
            String name = words[1];
            String email = words[2];

            ByteString rowKey = ByteString.copyFromUtf8(key);
            Mutation put = new Put(rowKey.toByteArray())
                    .addColumn("info".getBytes(),"name".getBytes(),name.getBytes())
                    .addColumn("info".getBytes(),"email".getBytes(),email.getBytes());

            List<Mutation> mutations = new ArrayList<>();
            mutations.add(put);

            c.output(KV.of(rowKey,(Iterable<Mutation>)mutations));
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
        @Description("driver")
        @Default.String("com.mysql.jdbc.Driver")
        String getDriver();
        void setDriver(String driver);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("uri")
        @Default.String("jdbc:mysql://10.0.82.164:3306/spark")
        String getUri();
        void setUri(String uri);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("userName")
        @Default.String("root")
        String getUserName();
        void setUserName(String userName);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("password")
        @Default.String("root")
        String getPassword();
        void setPassword(String password);

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("tableName")
        @Default.String("beam")
        String getTableName();
        void setTableName(String tableName);



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
          .apply(ParDo.of(new PrintFn<String>()))
          .apply(JdbcIO.<String>write()
                  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                options.getDriver(),options.getUri())
                                .withUsername(options.getUserName())
                                .withPassword(options.getPassword()))
                  .withStatement(String.format("insert into %s values(?,?,?)",options.getTableName()))
                  .withPreparedStatementSetter(
                          new JdbcIO.PreparedStatementSetter<String>(){

                              public void setParameters(String element, java.sql.PreparedStatement query) throws Exception {

                                  String[] words = element.split(" ");
                                  query.setInt(1,Integer.parseInt(words[0]));
                                  query.setString(2,words[1]);
                                  query.setString(3,words[2]);
                              }
                  }));

          //.apply("singeRow", ParDo.of(new ExtractFn())).setCoder(HBaseIO.WRITE_CODER)
          //.apply("write", HBaseIO.write().withConfiguration(conf).withTableId("beam"));


        p.run();

    }
}
