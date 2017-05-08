package cn.cnic.bigdata.chain;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

/**
 * Created by xjzhu on 17/5/2.
 */
public class JDBCWriteTransformer extends BaseTransformer {

    private String driver;
    private String uri;
    private String userName;
    private String password;
    private String tableName;

    public JDBCWriteTransformer(String name){

        super(name, TransformerType.JDBC);

    }

    public String getDriver() {
        return driver;
    }

    public JDBCWriteTransformer setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    public String getUri() {
        return uri;
    }

    public JDBCWriteTransformer setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public JDBCWriteTransformer setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JDBCWriteTransformer setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public JDBCWriteTransformer setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public JdbcIO.Write<String> construct(){

        return JdbcIO.<String>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        this.getDriver(),this.getUri())
                        .withUsername(this.getUserName())
                        .withPassword(this.getPassword()))
                .withStatement(String.format("insert into %s values(?,?,?)",this.getTableName()))
                .withPreparedStatementSetter(
                        new JdbcIO.PreparedStatementSetter<String>(){

                            public void setParameters(String element, java.sql.PreparedStatement query) throws Exception {

                                String[] words = element.split(" ");
                                query.setInt(1,Integer.parseInt(words[0]));
                                query.setString(2,words[1]);
                                query.setString(3,words[2]);
                            }
                        });

    }
}
