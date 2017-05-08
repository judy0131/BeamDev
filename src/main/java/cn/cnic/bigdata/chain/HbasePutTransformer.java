package cn.cnic.bigdata.chain;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xjzhu on 17/4/19.
 */
public class HbasePutTransformer extends BaseTransformer {



    public HbasePutTransformer(String name){

        super(name,TransformerType.HBASEPUT);

    }


    public <InputT, OutputT> ParDo.SingleOutput<String, KV<ByteString, Iterable<Mutation>>> construct(){

        return ParDo.of(new ExtractFn());
    }


    private static class ExtractFn extends DoFn<String, KV<ByteString, Iterable<Mutation>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(" ");
            String key = words[0];
            String name = words[1];
            String email = words[2];

            //System.out.println("key:" + key + "\tname:" + name + "\temail:" + email);

            ByteString rowKey = ByteString.copyFromUtf8(key);
            Mutation put = new Put(rowKey.toByteArray())
                    .addColumn("info".getBytes(),"name".getBytes(),name.getBytes())
                    .addColumn("info".getBytes(),"email".getBytes(),email.getBytes());

            List<Mutation> mutations = new ArrayList<>();
            mutations.add(put);

            c.output(KV.of(rowKey,(Iterable<Mutation>)mutations));
        }
    }

}
