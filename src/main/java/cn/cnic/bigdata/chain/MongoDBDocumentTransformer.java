package cn.cnic.bigdata.chain;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;

/**
 * Created by xjzhu on 17/5/2.
 */
public class MongoDBDocumentTransformer extends BaseTransformer {


    public MongoDBDocumentTransformer(String name){

        super(name, TransformerType.MONGODBDOCUMENT);
    }

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

    public ParDo.SingleOutput<String, Document> construct(){

        return ParDo.of(new parseDocument<String>());
    }

}
