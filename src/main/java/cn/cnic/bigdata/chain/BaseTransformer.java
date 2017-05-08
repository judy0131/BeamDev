package cn.cnic.bigdata.chain;

import com.google.protobuf.ByteString;
import com.sun.tools.javac.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Created by xjzhu on 17/4/18.
 */


public class BaseTransformer {

    private String name;
    private TransformerType type;

    public BaseTransformer(){

    }

    public BaseTransformer(String name, TransformerType type){
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TransformerType getType() {
        return type;
    }

    public void setType(TransformerType type) {
        this.type = type;
    }

//    public <InputT, OutputT> PTransform<PCollection<? extends InputT>, PCollection<OutputT>> construct(){
//
//        return null;
//
//    }

}
