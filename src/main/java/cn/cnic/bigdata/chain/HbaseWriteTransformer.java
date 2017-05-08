package cn.cnic.bigdata.chain;

import com.codahale.metrics.MetricRegistryListener;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by xjzhu on 17/4/28.
 */
public class HbaseWriteTransformer extends BaseTransformer {


    private String hmaster;
    private String zookeeperQuorum;
    private String zookeeperClientPort;
    private String table;

    public HbaseWriteTransformer(String name){
        super(name, TransformerType.HBASEWRITE);
    }

    public String getHmaster() {
        return hmaster;
    }

    public HbaseWriteTransformer setHmaster(String hmaster) {
        this.hmaster = hmaster;
        return this;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public HbaseWriteTransformer setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
        return this;
    }

    public String getZookeeperClientPort() {
        return zookeeperClientPort;
    }

    public HbaseWriteTransformer setZookeeperClientPort(String zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
        return this;
    }

    public String getTable() {
        return table;
    }

    public HbaseWriteTransformer setTable(String table) {
        this.table = table;
        return this;
    }

    public HBaseIO.Write construct(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master",this.getHmaster());
        conf.set("hbase.zookeeper.quorum",this.getZookeeperQuorum());
        conf.set("hbase.zookeeper.property.clientPort",this.getZookeeperClientPort());
        return HBaseIO.write().withConfiguration(conf).withTableId(this.getTable());

    }
}
