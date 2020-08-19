package Collection;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

/**
 * 连接MongoDB
 */
public class ConnectMongoDB {

    public static MongoDatabase connectMongoDatabase(){
        String host = "58.20.41.71";
        String username = "admin";
        String password = "Yaojiandan0807";
        String datebase = "admin";

        List<ServerAddress> addresses = new ArrayList<>();
        ServerAddress serverAddress = new ServerAddress(host,27017);
        addresses.add(serverAddress);

        List<MongoCredential> credentials = new ArrayList<>();
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(username,datebase,password.toCharArray());
        credentials.add(mongoCredential);

        MongoClientOptions.Builder builder= new MongoClientOptions.Builder();
        builder.connectTimeout(1500);
        builder.socketTimeout(1500);
        builder.serverSelectionTimeout(1500);

        MongoClientOptions options=builder.build();

        MongoClient mongoClient =new MongoClient(addresses,credentials);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("kang_hui_de_pharmacy");

        DB database = mongoClient.getDB("kang_hui_de_pharmacy");   //用以查询具体字段

        return mongoDatabase;
    }
    public static DB connectMongo(){
        String host = "58.20.41.71";
        String username = "admin";
        String password = "Yaojiandan0807";
        String datebase = "admin";

        List<ServerAddress> addresses = new ArrayList<>();
        ServerAddress serverAddress = new ServerAddress(host,27017);
        addresses.add(serverAddress);

        List<MongoCredential> credentials = new ArrayList<>();
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(username,datebase,password.toCharArray());
        credentials.add(mongoCredential);

//        MongoClientOptions.Builder builder= new MongoClientOptions.Builder();
//        builder.connectTimeout(1000);
//        builder.socketTimeout(1000);
//        builder.serverSelectionTimeout(1000);
//
//        MongoClientOptions options=builder.build();

        MongoClient mongoClient =new MongoClient(addresses,credentials,MongoClientOptions.builder()
//                .minHeartbeatFrequency(25)
//                .heartbeatSocketTimeout(3000)
                .socketKeepAlive(true)
                .socketTimeout(0)
                .maxConnectionIdleTime(0)
                .build());

        MongoDatabase mongoDatabase = mongoClient.getDatabase("kang_hui_de_pharmacy");

        DB database = mongoClient.getDB("kang_hui_de_pharmacy");   //用以查询具体字段


        return database;
    }

}
