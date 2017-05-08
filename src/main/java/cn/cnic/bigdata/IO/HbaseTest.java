package cn.cnic.bigdata.IO;


import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xjzhu on 17/4/13.
 */
public class HbaseTest {

    private static KV<ByteString, Iterable<Mutation>> makeWrite(String key, String value){

        ByteString rowKey = ByteString.copyFromUtf8(key);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(makeMutation(key,value));
        return KV.of(rowKey,(Iterable<Mutation>)mutations);

    }

    private static Mutation makeMutation(String key, String value){
        ByteString rowKey =  ByteString.copyFromUtf8(key);
        return new Put(rowKey.toByteArray())
                .addColumn("info".getBytes(),"name".getBytes(),value.getBytes())
                .addColumn("info".getBytes(),"email".getBytes(),(value+"@cnic.cn").getBytes());

    }

    public interface HbaseOptions extends PipelineOptions{

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("key")
        @Default.String("123456")
        String getKey();
        void setKey(String key);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("value")
        @Validation.Required
        String getValue();
        void setValue(String value);

    }
    public static void main(String [] args){
        HbaseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(HbaseOptions.class);
        Pipeline  p = Pipeline.create(options);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master","10.0.82.164:16010");
        conf.set("hbase.zookeeper.quorum","10.0.82.164,10.0.82.166,10.0.82.167");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        String key = options.getKey();
        String value = options.getValue();

        p.apply("singeRow", Create.of(makeWrite(key,value)).withCoder(HBaseIO.WRITE_CODER))
                .apply("write", HBaseIO.write().withConfiguration(conf).withTableId("beam"));

        p.run().waitUntilFinish();

    }
}
