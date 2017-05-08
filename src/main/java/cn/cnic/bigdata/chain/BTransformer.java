package cn.cnic.bigdata.chain;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by xjzhu on 17/4/28.
 */
public  class BTransformer<InputT, OutputT> extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
        return null;
    }

    private String name;
    private TransformerType type;

    public BTransformer(){

    }

    public BTransformer(String name, TransformerType type){
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

}
