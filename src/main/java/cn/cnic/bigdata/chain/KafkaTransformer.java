package cn.cnic.bigdata.chain;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xjzhu on 17/4/18.
 */
public class KafkaTransformer extends BaseTransformer {

    private String broker;
    private String topic;

    public KafkaTransformer(String name){

        super(name,TransformerType.KAFKA);

    }

    public PTransform<PBegin, PCollection<KV<String, String>>>  construct(){

        List<String> topics = ImmutableList.of(this.getTopic());

        return KafkaIO.<String, String>read()
                .withBootstrapServers(this.getBroker())
                .withTopics(topics)
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of())
                .withoutMetadata();
    }

    public KafkaTransformer setBroker(String broker) {
        this.broker = broker;
        return this;
    }

    public String getBroker() {
        return broker;
    }

    public String getTopic() {
        return topic;
    }

    public KafkaTransformer setTopic(String topic) {
        this.topic = topic;
        return this;
    }
}
