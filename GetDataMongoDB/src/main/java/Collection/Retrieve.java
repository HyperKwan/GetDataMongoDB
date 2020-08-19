package Collection;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

public class Retrieve {
    //查找集合中的所有文档
    @Test
    public void findTest(){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = null ; //ConnectMongoDB.connectMongo();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection("user");
        //查找集合中的所有文档
        FindIterable findIterable = collection.find();
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }


    //指定查询过滤器查询
    @Test
    public void FilterfindTest(){
        //获取数据库连接对象
        DB database = ConnectMongoDB.connectMongo();//获取数据库
        DBCollection collection = database.getCollection("g_sale_pay");//集合名
        DBCursor cursor = collection.find();
        while (cursor.hasNext()) {
//            System.out.println(cursor.next());
            BasicDBObject bobject = (BasicDBObject) cursor.next();


            System.out.println(bobject.getString("saleno"));
        }
    }

    //取出查询到的第一个文档
    @Test
    public void findFirstTest(){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = null ; //ConnectMongoDB.connectMongo();
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection("g_sale_pay");
        //查找集合中的所有文档
        FindIterable findIterable = collection.find();
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        System.out.println(document);
    }

}
