package cn.cnic.bigdata.chain;

import org.apache.beam.sdk.io.mongodb.MongoDbIO;

/**
 * Created by xjzhu on 17/5/2.
 */
public class MongoDBWriteTransformer extends BaseTransformer{

    private String uri;
    private String db;
    private String collection;

    public MongoDBWriteTransformer(String name){

        super(name, TransformerType.MONGODBWRITE);
    }

    public String getUri() {
        return uri;
    }

    public MongoDBWriteTransformer setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getDb() {
        return db;
    }

    public MongoDBWriteTransformer setDb(String db) {
        this.db = db;
        return this;
    }

    public String getCollection() {
        return collection;
    }

    public MongoDBWriteTransformer setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public MongoDbIO.Write construct(){

        return MongoDbIO.write()
                .withUri(this.getUri())
                .withDatabase(this.getDb())
                .withCollection(this.getCollection());

    }
}
