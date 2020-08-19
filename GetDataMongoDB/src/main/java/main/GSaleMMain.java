package main;

import Collection.ConnectMongoDB;
import Collection.ConnectMySql;
import Entity.TbIpGSaleM;

import Util.Util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GSaleMMain extends Thread {


    public static void main(String args[]) {
//        GSaleMMain dgSaleMMain = new GSaleMMain();
//        dgSaleMMain.start();
//
//    }
//
//    public void run(){

        List<TbIpGSaleM> list = new ArrayList<>();

        SimpleDateFormat sdfM = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /*******************连接MongoDB*******************/
        //获取数据库连接对象
//        MongoDatabase mongoDatabase =  ConnectMongoDB.connectMongoDatabase();
//        //获取集合
//        MongoCollection<Document> collection = mongoDatabase.getCollection("g_sale_m");
//        //查找集合中的所有文档
//        FindIterable findIterable = collection.find();
//        MongoCursor cursor = findIterable.iterator();

        DB database = ConnectMongoDB.connectMongo();//获取数据库
        DBCollection collection = database.getCollection("g_sale_m");//集合名
        DBCursor cursor = collection.find();
        System.out.println(cursor.count());
        BasicDBObject dbObject;
//        JSONObject dbObject ;
        int x = 0;
        long start = System.currentTimeMillis();

//        while (cursor.hasNext()) {
//            dbObject = (BasicDBObject) cursor.next();
//            TbIpGSaleM gSaleM = new TbIpGSaleM();
//            gSaleM.setId(dbObject.getString("_id"));
//            gSaleM.setAccdate(dbObject.getString("accdate"));
//            gSaleM.setBak1(dbObject.getString("bak1"));
//            gSaleM.setBak2(dbObject.getString("bak2"));
//            gSaleM.setBusno(dbObject.getString("busno"));
//            gSaleM.setDiscounter(dbObject.getString("discounter"));
//            gSaleM.setFinaltime(dbObject.getString("finaltime"));
//            gSaleM.setLoss(dbObject.getString("loss"));
//            gSaleM.setMember(dbObject.getString("member"));
//            gSaleM.setNetsum(dbObject.getString("netsum"));
//            gSaleM.setPayee(dbObject.getString("payee"));
//            gSaleM.setPosno(dbObject.getString("posno"));
//            gSaleM.setPrecash(dbObject.getString("precash"));
//            gSaleM.setReturner(dbObject.getString("returner"));
//            gSaleM.setSaleno(dbObject.getString("saleno"));
//            gSaleM.setStamp(dbObject.getString("stamp"));
//            gSaleM.setStarttime(dbObject.getString("starttime"));
//            gSaleM.setStdsum(dbObject.getString("stdsum"));
//            gSaleM.setWarranter1(dbObject.getString("warranter1"));
//            gSaleM.setWarranter2(dbObject.getString("warranter2"));
//            gSaleM.setWarranter3(dbObject.getString("warranter3"));
//            gSaleM.setWarranter4(dbObject.getString("warranter4"));
//
//            list.add(gSaleM);
//        }
        System.out.println("数据读取完成");
        long end = System.currentTimeMillis();
        System.out.println("读取数据库总共耗时：" + (end - start) / 60000 + "min");

        /*******************连接MongoDB*******************/


        /*******************连接Mysql*******************/

        Connection conn = ConnectMySql.getCon();
        PreparedStatement ps = null;
        String sql = "Insert Into TB_IP_GSaleM (`_id`,accdate,bak1,bak2,busno,discounter,finaltime,loss,member,netsum,payee,posno,precash,returner,saleno,stamp,starttime,stdsum,warranter1,warranter2,warranter3,warranter4,ModificationDate)" +
                " Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String del = "Truncate TB_IP_GSaleM";
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
//                TbIpGSaleM gSaleM = list.get(i);
//                ps.setString(1, gSaleM.getId());
//                ps.setString(2, gSaleM.getAccdate());
//                ps.setString(3, gSaleM.getBak1());
//                ps.setString(4, gSaleM.getBak2());
//                ps.setString(5, gSaleM.getBusno());
//                ps.setString(6, gSaleM.getDiscounter());
//                ps.setString(7, gSaleM.getFinaltime());
//                ps.setString(8, gSaleM.getLoss());
//                ps.setString(9, gSaleM.getMember());
//                ps.setString(10, gSaleM.getNetsum());
//                ps.setString(11, gSaleM.getPayee());
//                ps.setString(12, gSaleM.getPosno());
//                ps.setString(13, gSaleM.getPrecash());
//                ps.setString(14, gSaleM.getReturner());
//                ps.setString(15, gSaleM.getSaleno());
//                ps.setString(16, gSaleM.getStamp());
//                ps.setString(17, gSaleM.getStarttime());
//                ps.setString(18, gSaleM.getStdsum());
//                ps.setString(19, gSaleM.getWarranter1());
//                ps.setString(20, gSaleM.getWarranter2());
//                ps.setString(21, gSaleM.getWarranter3());
//                ps.setString(22, gSaleM.getWarranter4());
            while (cursor.hasNext()) {
                dbObject = (BasicDBObject) cursor.next();
                ps.setString(1, dbObject.getString("_id"));
                ps.setString(2, Util.DateFormat(dbObject.getString("accdate")));
                ps.setString(3, dbObject.getString("bak1"));
                ps.setString(4, dbObject.getString("bak2"));
                ps.setString(5, dbObject.getString("busno"));
                ps.setString(6, dbObject.getString("discounter"));
                ps.setString(7, Util.DateFormat(dbObject.getString("finaltime")));
                ps.setString(8, dbObject.getString("loss"));
                ps.setString(9, dbObject.getString("member"));
                ps.setString(10, dbObject.getString("netsum"));
                ps.setString(11, dbObject.getString("payee"));
                ps.setString(12, dbObject.getString("posno"));
                ps.setString(13, dbObject.getString("precash"));
                ps.setString(14, dbObject.getString("returner"));
                ps.setString(15, dbObject.getString("saleno"));
                ps.setString(16, dbObject.getString("stamp"));
                ps.setString(17, Util.DateFormat(dbObject.getString("starttime")));
                ps.setString(18, dbObject.getString("stdsum"));
                ps.setString(19, dbObject.getString("warranter1"));
                ps.setString(20, dbObject.getString("warranter2"));
                ps.setString(21, dbObject.getString("warranter3"));
                ps.setString(22, dbObject.getString("warranter4"));

                ps.setString(23,sdfM.format(new Date()));


                ps.addBatch();//再添加一次预定义参数
                if ((c + 1) % 5000 == 0) {//每1000条数据提交一次
                    System.out.println("成功写入" + (c + 1) + "条数据");
                    ps.executeBatch();//执行批量执行
                    conn.commit();//提交数据
                }                        //提交剩余的数据
                c++;
//                System.out.println(c);

            }
            if ((c + 1) % 5000 != 0) {
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
