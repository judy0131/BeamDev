package cn.cnic.bigdata.chain;

import com.codahale.metrics.MetricRegistryListener;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Created by xjzhu on 17/4/28.
 */
public class PrintTransformer extends BaseTransformer {

    public PrintTransformer(String name){

        super(name,TransformerType.PRINT);

    }

    private static class PrintFn<T> extends DoFn<T, T> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element().toString());
            c.output(c.element());
        }
    }


    public <T> ParDo.SingleOutput<T,T> construct(){

        return ParDo.of(new PrintFn<T>());
    }
}
