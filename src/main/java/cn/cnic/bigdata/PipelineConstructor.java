package cn.cnic.bigdata;

import cn.cnic.bigdata.chain.*;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.apache.hadoop.hbase.client.Mutation;
import org.bson.Document;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xjzhu on 17/4/18.
 */
public class PipelineConstructor {

    private Pipeline pipeline;
    private KafkaTransformer kafkaTransformer;

    public PipelineConstructor(){

//        CommonOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
//                .as(CommonOptions.class);
//        this.pipeline = Pipeline.create(options);

        this.kafkaTransformer = new KafkaTransformer("kafkaT")
                .setBroker("10.0.82.164:9092,10.0.82.166:9092,10.0.82.167:9092")
                .setTopic("beam");

    }


    public interface CommonOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("hbaseMaster")
        @Default.String("10.0.82.164:16010")
        String getHbaseMaster();
        void setHbaseMaster(String hbaseMaster);

    }


    public static void main(String [] args){


        //define pipeline
        CommonOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CommonOptions.class);
        Pipeline pipeline = Pipeline.create(options);


        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~transformer define~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //kafka transformer
        KafkaTransformer kafkaTransformer = new KafkaTransformer("kafkaTrans")
                .setBroker("10.0.82.164:9092,10.0.82.166:9092,10.0.82.167:9092")
                .setTopic("beam");

        //print transformer
        PrintTransformer printTransformer = new PrintTransformer("printTrans");

        //HbasePut transformer
        HbasePutTransformer hbasePutTransformer = new HbasePutTransformer("hbasePutTrans");

        //HbaseWrite transformer
        HbaseWriteTransformer hbaseWriteTransformer = new HbaseWriteTransformer("hbaseWriteTrans")
                .setHmaster("10.0.82.164:16010")
                .setZookeeperQuorum("10.0.82.164,10.0.82.166,10.0.82.167")
                .setZookeeperClientPort("2181")
                .setTable("beam");

        //MongodbDocument transformer
        MongoDBDocumentTransformer mongoDBDocumentTransformer = new MongoDBDocumentTransformer("mongoDBDocumentTrans");

        //MongodbWrite transformer
        MongoDBWriteTransformer mongoDBWriteTransformer = new MongoDBWriteTransformer("mongodbWriteTrans")
                .setUri("mongodb://10.0.82.172:27017")
                .setDb("spark")
                .setCollection("beam");


        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~pipeline define~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //kafka to hbase pipeline
        List<BaseTransformer> hbaseList = new ArrayList<BaseTransformer>();
        hbaseList.add(kafkaTransformer);
        hbaseList.add(printTransformer);
        hbaseList.add(hbasePutTransformer);
        hbaseList.add(hbaseWriteTransformer);

        //kafka to mongodb pipeline
        List<BaseTransformer> mongoList = new ArrayList<BaseTransformer>();
        mongoList.add(kafkaTransformer);
        mongoList.add(printTransformer);
        mongoList.add(mongoDBDocumentTransformer);
        mongoList.add(mongoDBWriteTransformer);

        //multiple transformer pipeline
        List<BaseTransformer> multipleTransformerList = new ArrayList<BaseTransformer>();
        multipleTransformerList.add(kafkaTransformer);
        multipleTransformerList.add(printTransformer);

        //multiple source pipeline
        List<BaseTransformer> multipleSourceList = new ArrayList<BaseTransformer>();
        multipleSourceList.add(kafkaTransformer);


        Object pCollectionOut = null;
        for (BaseTransformer trans : multipleTransformerList){

            switch (trans.getType()){

                case KAFKA:
                    KafkaTransformer kafkaTransformer1 = (KafkaTransformer)trans;
                    PCollection<String> pCollection = pipeline.apply(kafkaTransformer1.construct()).apply(Values.<String>create());
                    pCollectionOut = pCollection;
                    break;

                case HBASEPUT:
                    HbasePutTransformer hbasePutTransformer1 = (HbasePutTransformer)trans;
                    pCollectionOut = ((PCollection<String>)pCollectionOut).apply(hbasePutTransformer1.construct()).setCoder(HBaseIO.WRITE_CODER);
                    break;

                case HBASEWRITE:
                    HbaseWriteTransformer hbaseWriteTransformer1 = (HbaseWriteTransformer) trans;
                    pCollectionOut = ((PCollection<KV<ByteString, Iterable<Mutation>>>)pCollectionOut).apply(hbaseWriteTransformer1.construct());
                    break;

                case MONGODBDOCUMENT:
                    MongoDBDocumentTransformer mdbT = (MongoDBDocumentTransformer)trans;
                    pCollectionOut = ((PCollection<String>)pCollectionOut).apply(mdbT.construct());
                    break;

                case MONGODBWRITE:
                    MongoDBWriteTransformer mdbwT = (MongoDBWriteTransformer)trans;
                    pCollectionOut = ((PCollection<Document>)pCollectionOut).apply(mdbwT.construct());
                    break;

                case JDBC:
                    break;

                case PRINT:
                    PrintTransformer printTransformer1 = (PrintTransformer)trans;
                    pCollectionOut = ((PCollection<String>)pCollectionOut).apply(printTransformer1.<String>construct());
                    break;

                default:
                    break;

            }
        }

//        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~multiple pipeline define~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//        ((PCollection<String>)pCollectionOut).apply(hbasePutTransformer.construct()).setCoder(HBaseIO.WRITE_CODER).apply(hbaseWriteTransformer.construct());
//        ((PCollection<String>)pCollectionOut).apply(mongoDBDocumentTransformer.construct()).apply(mongoDBWriteTransformer.construct());

        PCollection<String> kafkaCollectionOut = pipeline.apply(kafkaTransformer.construct()).apply(Values.<String>create());
        PCollection<String> fileCollectionOut = pipeline.apply("localFile",TextIO.Read.from("text.txt"));
        //PCollection<String> fileCollectionOut1 = pipeline.apply("localFile",TextIO.Read.from("text1.txt"));

        PCollectionList.of(kafkaCollectionOut).and(fileCollectionOut).apply(Flatten.<String>pCollections()).apply(printTransformer.<String>construct());


        pipeline.run();

//        pipeline.apply(kafkaTransformer.construct())
//                .apply(Values.<String>create())
//                //.apply(printTransformer.<String>construct())
//                .apply(hbasePutTransformer.construct()).setCoder(HBaseIO.WRITE_CODER)
//                .apply(hbaseWriteTransformer.construct());
//
//        pipeline.run();
    }
}
