package main;

import Collection.ConnectMongoDB;
import Collection.ConnectMySql;
import Entity.TbIpGStoreC;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GStoreCMain extends Thread {


    public static void main(String args[]) {
        GStoreCMain gStoreCMain = new GStoreCMain();
        gStoreCMain.start();

    }
    public void run(){

        List<TbIpGStoreC> list = new ArrayList<>();

        SimpleDateFormat sdfM = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /*******************连接MongoDB*******************/
        //获取数据库连接对象
//        MongoDatabase mongoDatabase = null ; //ConnectMongoDB.connectMongo();
////        //获取集合
////        MongoCollection<Document> collection = null ; //ConnectMongoDB.connectMongo().getCollection("g_sale_pay");
////        //查找集合中的所有文档
////        FindIterable findIterable = collection.find();
////        MongoCursor cursor = findIterable.iterator();

        DB database = ConnectMongoDB.connectMongo();//获取数据库
        DBCollection collection = database.getCollection("g_store_c");//集合名
        DBCursor cursor = collection.find();
//        cursor.limit(5);
        System.out.println(cursor.count());
        BasicDBObject dbObject;
        int x = 0;
        long start = System.currentTimeMillis();

        while (cursor.hasNext()) {
            dbObject = (BasicDBObject) cursor.next();
            TbIpGStoreC gStoreC = new TbIpGStoreC();
            gStoreC.setId(dbObject.getString("_id"));
            gStoreC.setAwaitqty(dbObject.getString("awaitqty"));
            gStoreC.setBatchno(dbObject.getString("batchno"));
            gStoreC.setBusno(dbObject.getString("busno"));
            gStoreC.setGoodscode(dbObject.getString("goodscode"));
            gStoreC.setGoodsqty(dbObject.getString("goodsqty"));
            gStoreC.setIdno(dbObject.getString("idno"));
            gStoreC.setStallno(dbObject.getString("stallno"));
            gStoreC.setStamp(dbObject.getString("stamp"));

            list.add(gStoreC);
        }
        System.out.println("数据读取完成");
        long end = System.currentTimeMillis();
        System.out.println("读取数据库总共耗时：" + (end - start) / 60000 + "min");

        /*******************连接MongoDB*******************/


        /*******************连接Mysql*******************/

        Connection conn = ConnectMySql.getConTest();
        PreparedStatement ps = null;
        String sql = "Insert Into TB_IP_GStoreC (`_id`,awaitqty,batchno,busno,goodscode,goodsqty,idno,stallno,stamp,ModificationDate) " +
                "Values (?,?,?,?,?,?,?,?,?,?)";
        String del = "Truncate TB_IP_GStoreC";
        int c = 0 ;
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(del);
            ps.executeUpdate();
            conn.commit();
            System.out.println("已删除历史数据");
            ps = conn.prepareStatement(sql);
            long startTime = System.currentTimeMillis();

            for(int i = 0 ;i<list.size();i++) {
                TbIpGStoreC gStoreC = list.get(i);
                ps.setString(1, gStoreC.getId());
                ps.setString(2, gStoreC.getAwaitqty());
                ps.setString(3, gStoreC.getBatchno());
                ps.setString(4, gStoreC.getBusno());
                ps.setString(5, gStoreC.getGoodscode());
                ps.setString(6, gStoreC.getGoodsqty());
                ps.setString(7, gStoreC.getIdno());
                ps.setString(8, gStoreC.getStallno());
                ps.setString(9, gStoreC.getStamp());
                ps.setString(10, sdfM.format(new Date()));


                ps.addBatch();//再添加一次预定义参数
                if ((c + 1) % 5000 == 0) {//每1000条数据提交一次
                    System.out.println("成功写入"+(c+1)+"条数据");
                    ps.executeBatch();//执行批量执行
                    conn.commit();//提交数据
                }                        //提交剩余的数据
                c++;
//                System.out.println(c);
            }
            if ((c+1) % 5000 != 0) {
                ps.executeBatch();//执行批量执行
                conn.commit();//提交数据
            }

            System.out.println("成功将"+list.size()+"条数据写入到数据库中");
            long endTime = System.currentTimeMillis();
            System.out.println("写入数据库总共耗时：" + (endTime - startTime) / 60000 + "min");

        } catch (Exception e) {
            System.out.println("写入数据库失败");
            e.printStackTrace();
        } finally {//无论是否执行成功,都得关闭链接
            try {
                cursor.close();
                ps.close();
                conn.close();              //关闭连接
                System.out.println("关闭流成功");
            } catch (SQLException e) {
                System.out.println("关闭流失败");
                e.printStackTrace();
            }
        }
    }
}