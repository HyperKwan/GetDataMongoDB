package main;

import Collection.ConnectMongoDB;
import Collection.ConnectMySql;
import Entity.TbIpGSaleC;

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

public class GSaleCMain extends Thread {


    public static void main(String args[]) {
        GSaleCMain gSaleCMain = new GSaleCMain();
        gSaleCMain.start();

    }

    public void run() {

        List<TbIpGSaleC> list = new ArrayList<>();

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
        DBCollection collection = database.getCollection("g_sale_c");//集合名
        DBCursor cursor = collection.find();
//        cursor.limit(5);
        System.out.println(cursor.count());
        BasicDBObject dbObject;
        int x = 0;
        long start = System.currentTimeMillis();

//        while (cursor.hasNext()) {
//            dbObject = (BasicDBObject) cursor.next();
//            TbIpGSaleC gSaleC = new TbIpGSaleC();
//            gSaleC.setAccdate(dbObject.getString("accdate"));
//            gSaleC.setAvgpurprice(dbObject.getString("avgpurprice"));
//            gSaleC.setBak1(dbObject.getString("bak1"));
//            gSaleC.setBatchno(dbObject.getString("batchno"));
//            gSaleC.setBusno(dbObject.getString("busno"));
//            gSaleC.setDisrate(dbObject.getString("disrate"));
//            gSaleC.setDistype(dbObject.getString("distype"));
//            gSaleC.setGoodscode(dbObject.getString("goodscode"));
//            gSaleC.setGoodsqty(dbObject.getString("goodsqty"));
//            gSaleC.setGroupid(dbObject.getString("groupid"));
//            gSaleC.setIsIntegralTimes(dbObject.getString("is_integral_times"));
//            gSaleC.setMessage(dbObject.getString("message"));
//            gSaleC.setMinprice(dbObject.getString("minprice"));
//            gSaleC.setMinqty(dbObject.getString("minqty"));
//            gSaleC.setNetprice(dbObject.getString("netprice"));
//            gSaleC.setPurprice(dbObject.getString("purprice"));
//            gSaleC.setRowid(dbObject.getString("rowid"));
//            gSaleC.setRowtype(dbObject.getString("rowtype"));
//            gSaleC.setSaleno(dbObject.getString("saleno"));
//            gSaleC.setSaler(dbObject.getString("saler"));
//            gSaleC.setStallno(dbObject.getString("stallno"));
//            gSaleC.setStdprice(dbObject.getString("stdprice"));
//            gSaleC.setStdtomin(dbObject.getString("stdtomin"));
//            gSaleC.setTimes(dbObject.getString("times"));
//            gSaleC.setIdno(dbObject.getString("idno"));
//            gSaleC.setId(dbObject.getString("_id"));
//            gSaleC.setMakeno(dbObject.getString("makeno"));
//            gSaleC.setInvalidate(dbObject.getString("invalidate"));
//            gSaleC.setPurtax(dbObject.getString("purtax"));
//            gSaleC.setPile(dbObject.getString("pile"));
//            gSaleC.setIsIntegral(dbObject.getString("is_integral"));
//
//            list.add(gSaleC);
//        }
        System.out.println("数据读取完成");
        long end = System.currentTimeMillis();
        System.out.println("读取数据库总共耗时：" + (end - start) / 60000 + "min");

        /*******************连接MongoDB*******************/


        /*******************连接Mysql*******************/

        Connection conn = ConnectMySql.getCon();
        PreparedStatement ps = null;
        String sql = "Insert Into TB_IP_GSaleC (accdate,avgpurprice,bak1,batchno,busno,disrate,distype,goodscode,goodsqty,groupid,is_integral_times,message,minprice,minqty,netprice,purprice,rowid,rowtype,saleno,saler,stallno,stdprice,stdtomin,times,idno,`_id`,makeno,invalidate,purtax,pile,is_integral,ModificationDate)" +
                " Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String del = "Truncate TB_IP_GSaleC";
        int c = 0;
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(del);
            ps.executeUpdate();
            conn.commit();
            System.out.println("已删除历史数据");
            ps = conn.prepareStatement(sql);
            long startTime = System.currentTimeMillis();

//            for (int i = 0; i < list.size(); i++) {
            while (cursor.hasNext()) {
                dbObject = (BasicDBObject) cursor.next();
//                TbIpGSaleC gSaleC = list.get(i);
//                ps.setString(1, gSaleC.getAccdate());
//                ps.setString(2, gSaleC.getAvgpurprice());
//                ps.setString(3, gSaleC.getBak1());
//                ps.setString(4, gSaleC.getBatchno());
//                ps.setString(5, gSaleC.getBusno());
//                ps.setString(6, gSaleC.getDisrate());
//                ps.setString(7, gSaleC.getDistype());
//                ps.setString(8, gSaleC.getGoodscode());
//                ps.setString(9, gSaleC.getGoodsqty());
//                ps.setString(10, gSaleC.getGroupid());
//                ps.setString(11, gSaleC.getIsIntegralTimes());
//                ps.setString(12, gSaleC.getMessage());
//                ps.setString(13, gSaleC.getMinprice());
//                ps.setString(14, gSaleC.getMinqty());
//                ps.setString(15, gSaleC.getNetprice());
//                ps.setString(16, gSaleC.getPurprice());
//                ps.setString(17, gSaleC.getRowid());
//                ps.setString(18, gSaleC.getRowtype());
//                ps.setString(19, gSaleC.getSaleno());
//                ps.setString(20, gSaleC.getSaler());
//                ps.setString(21, gSaleC.getStallno());
//                ps.setString(22, gSaleC.getStdprice());
//                ps.setString(23, gSaleC.getStdtomin());
//                ps.setString(24, gSaleC.getTimes());
//                ps.setString(25, gSaleC.getIdno());
//                ps.setString(26, gSaleC.getId());
//                ps.setString(27, gSaleC.getMakeno());
//                ps.setString(28, gSaleC.getInvalidate());
//                ps.setString(29, gSaleC.getPurtax());
//                ps.setString(30, gSaleC.getPile());
//                ps.setString(31, gSaleC.getIsIntegral());
//                ps.setString(32, sdfM.format(new Date()));
                ps.setString(1, Util.DateFormat(dbObject.getString("accdate")));
                ps.setString(2, dbObject.getString("avgpurprice"));
                ps.setString(3, dbObject.getString("bak1"));
                ps.setString(4, dbObject.getString("batchno"));
                ps.setString(5, dbObject.getString("busno"));
                ps.setString(6, dbObject.getString("disrate"));
                ps.setString(7, dbObject.getString("distype"));
                ps.setString(8, dbObject.getString("goodscode"));
                ps.setString(9, dbObject.getString("goodsqty"));
                ps.setString(10, dbObject.getString("groupid"));
                ps.setString(11, dbObject.getString("is_integral_times"));
                ps.setString(12, dbObject.getString("message"));
                ps.setString(13, dbObject.getString("minprice"));
                ps.setString(14, dbObject.getString("minqty"));
                ps.setString(15, dbObject.getString("netprice"));
                ps.setString(16, dbObject.getString("purprice"));
                ps.setString(17, dbObject.getString("rowid"));
                ps.setString(18, dbObject.getString("rowtype"));
                ps.setString(19, dbObject.getString("saleno"));
                ps.setString(20, dbObject.getString("saler"));
                ps.setString(21, dbObject.getString("stallno"));
                ps.setString(22, dbObject.getString("stdprice"));
                ps.setString(23, dbObject.getString("stdtomin"));
                ps.setString(24, dbObject.getString("times"));
                ps.setString(25, dbObject.getString("idno"));
                ps.setString(26, dbObject.getString("_id"));
                ps.setString(27, dbObject.getString("makeno"));
                ps.setString(28, Util.DateFormat(dbObject.getString("invalidate")));
                ps.setString(29, dbObject.getString("purtax"));
                ps.setString(30, dbObject.getString("pile"));
                ps.setString(31, dbObject.getString("is_integral"));
                ps.setString(32, sdfM.format(new Date()));


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

            System.out.println("成功将" + cursor.count() + "条数据写入到数据库中");
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