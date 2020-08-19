package main;

import Collection.ConnectMongoDB;
import Collection.ConnectMySql;
import Entity.TbIpGStoreM;
import Util.Util;
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

public class GStoreMMain extends Thread {


    public static void main(String args[]) {
        GStoreMMain gStoreMMain = new GStoreMMain();
        gStoreMMain.start();

    }
    public void run(){

        List<TbIpGStoreM> list = new ArrayList<>();

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
        DBCollection collection = database.getCollection("g_store_m");//集合名
        DBCursor cursor = collection.find();
//        cursor.limit(5);
        System.out.println(cursor.count());
        BasicDBObject dbObject;
        int x = 0;
        long start = System.currentTimeMillis();

//        while (cursor.hasNext()) {
//            dbObject = (BasicDBObject) cursor.next();
//            TbIpGStoreM gStoreM = new TbIpGStoreM();
//            gStoreM.setId(dbObject.getString("_id"));
//            gStoreM.setAllocqty(dbObject.getString("allocqty"));
//            gStoreM.setAlltype(dbObject.getString("alltype"));
//            gStoreM.setAutodist(dbObject.getString("autodist"));
//            gStoreM.setBusno(dbObject.getString("busno"));
//            gStoreM.setComputetype(dbObject.getString("computetype"));
//            gStoreM.setGoodscode(dbObject.getString("goodscode"));
//            gStoreM.setLastdistexecdate(dbObject.getString("lastdistexecdate"));
//            gStoreM.setLastdistqty(dbObject.getString("lastdistqty"));
//            gStoreM.setLastm2Qty(dbObject.getString("lastm2qty"));
//            gStoreM.setLastm3Qty(dbObject.getString("lastm3qty"));
//            gStoreM.setLastmqty(dbObject.getString("lastmqty"));
//            gStoreM.setLastsaledate(dbObject.getString("lastsaledate"));
//            gStoreM.setLastyqty(dbObject.getString("lastyqty"));
//            gStoreM.setMaxday(dbObject.getString("maxday"));
//            gStoreM.setMaxqty(dbObject.getString("maxqty"));
//            gStoreM.setMinday(dbObject.getString("minday"));
//            gStoreM.setMinqty(dbObject.getString("minqty"));
//            gStoreM.setStamp(dbObject.getString("stamp"));
//            gStoreM.setStorepurprice(dbObject.getString("storepurprice"));
//            gStoreM.setSumawaitqty(dbObject.getString("sumawaitqty"));
//            gStoreM.setSumpendingqty(dbObject.getString("sumpendingqty"));
//            gStoreM.setSumqty(dbObject.getString("sumqty"));
//
//            list.add(gStoreM);
//        }
        System.out.println("数据读取完成");
        long end = System.currentTimeMillis();
        System.out.println("读取数据库总共耗时：" + (end - start) / 60000 + "min");

        /*******************连接MongoDB*******************/


        /*******************连接Mysql*******************/

        Connection conn = ConnectMySql.getConTest();
        PreparedStatement ps = null;
        String sql = "Insert Into TB_IP_GStoreM (`_id`,allocqty,alltype,autodist,busno,computetype,goodscode,lastdistexecdate,lastdistqty,lastm2qty,lastm3qty,lastmqty,lastsaledate,lastyqty,maxday,maxqty,minday,minqty,stamp,storepurprice,sumawaitqty,sumpendingqty,sumqty,ModificationDate)" +
                " Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String del = "Truncate TB_IP_GStoreM";
        int c = 0 ;
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(del);
            ps.executeUpdate();
            conn.commit();
            System.out.println("已删除历史数据");
            ps = conn.prepareStatement(sql);
            long startTime = System.currentTimeMillis();

//            for(int i = 0 ;i<list.size();i++) {
//                TbIpGStoreM gStoreM = list.get(i);
//                ps.setString(1, gStoreM.getId());
//                ps.setString(2, gStoreM.getAllocqty());
//                ps.setString(3, gStoreM.getAlltype());
//                ps.setString(4, gStoreM.getAutodist());
//                ps.setString(5, gStoreM.getBusno());
//                ps.setString(6, gStoreM.getComputetype());
//                ps.setString(7, gStoreM.getGoodscode());
//                ps.setString(8, gStoreM.getLastdistexecdate());
//                ps.setString(9, gStoreM.getLastdistqty());
//                ps.setString(10, gStoreM.getLastm2Qty());
//                ps.setString(11, gStoreM.getLastm3Qty());
//                ps.setString(12, gStoreM.getLastmqty());
//                ps.setString(13, gStoreM.getLastsaledate());
//                ps.setString(14, gStoreM.getLastyqty());
//                ps.setString(15, gStoreM.getMaxday());
//                ps.setString(16, gStoreM.getMaxqty());
//                ps.setString(17, gStoreM.getMinday());
//                ps.setString(18, gStoreM.getMinqty());
//                ps.setString(19, gStoreM.getStamp());
//                ps.setString(20, gStoreM.getStorepurprice());
//                ps.setString(21, gStoreM.getSumawaitqty());
//                ps.setString(22, gStoreM.getSumpendingqty());
//                ps.setString(23, gStoreM.getSumqty());

            while (cursor.hasNext()) {
                dbObject = (BasicDBObject) cursor.next();
                ps.setString(1, dbObject.getString("_id"));
                ps.setString(2, dbObject.getString("allocqty"));
                ps.setString(3, dbObject.getString("alltype"));
                ps.setString(4, dbObject.getString("autodist"));
                ps.setString(5, dbObject.getString("busno"));
                ps.setString(6, dbObject.getString("computetype"));
                ps.setString(7, dbObject.getString("goodscode"));
                ps.setString(8, Util.DateFormat(dbObject.getString("lastdistexecdate")));
                ps.setString(9, dbObject.getString("lastdistqty"));
                ps.setString(10, dbObject.getString("lastm2qty"));
                ps.setString(11, dbObject.getString("lastm3qty"));
                ps.setString(12, dbObject.getString("lastmqty"));
                ps.setString(13,Util.DateFormat( dbObject.getString("lastsaledate")));
                ps.setString(14, dbObject.getString("lastyqty"));
                ps.setString(15, dbObject.getString("maxday"));
                ps.setString(16, dbObject.getString("maxqty"));
                ps.setString(17, dbObject.getString("minday"));
                ps.setString(18, dbObject.getString("minqty"));
                ps.setString(19, dbObject.getString("stamp"));
                ps.setString(20, dbObject.getString("storepurprice"));
                ps.setString(21, dbObject.getString("sumawaitqty"));
                ps.setString(22, dbObject.getString("sumpendingqty"));
                ps.setString(23, dbObject.getString("sumqty"));
                ps.setString(24, sdfM.format(new Date()));


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
