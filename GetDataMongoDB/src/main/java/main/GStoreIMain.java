package main;

import Collection.ConnectMongoDB;
import Collection.ConnectMySql;
import Entity.TbIpGStoreI;
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

public class GStoreIMain extends Thread {


    public static void main(String args[]) {
        GStoreIMain gStoreIMain = new GStoreIMain();
        gStoreIMain.start();

    }
    public void run(){

        List<TbIpGStoreI> list = new ArrayList<>();

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
        DBCollection collection = database.getCollection("g_store_i");//集合名
        DBCursor cursor = collection.find();
//        cursor.limit(5);
        System.out.println(cursor.count());
        BasicDBObject dbObject;
        int x = 0;
        long start = System.currentTimeMillis();

//        while (cursor.hasNext()) {
//            dbObject = (BasicDBObject) cursor.next();
//            TbIpGStoreI gStoreI = new TbIpGStoreI();
//            gStoreI.setId(dbObject.getString("_id"));
//            gStoreI.setBackprice(dbObject.getString("backprice"));
//            gStoreI.setBatchno(dbObject.getString("batchno"));
//            gStoreI.setBioavailability(dbObject.getString("Bioavailability"));
//            gStoreI.setBuyer(dbObject.getString("buyer"));
//            gStoreI.setDistprice(dbObject.getString("distprice"));
//            gStoreI.setExecdate(dbObject.getString("execdate"));
//            gStoreI.setFlag(dbObject.getString("flag"));
//            gStoreI.setFlag2(dbObject.getString("flag2"));
//            gStoreI.setGoodscode(dbObject.getString("goodscode"));
//            gStoreI.setIdno(dbObject.getString("idno"));
//            gStoreI.setInitbusno(dbObject.getString("initbusno"));
//            gStoreI.setInvalidate(dbObject.getString("invalidate"));
//            gStoreI.setInvalidateChar(dbObject.getString("invalidate_char"));
//            gStoreI.setInvalidateo(dbObject.getString("invalidateo"));
//            gStoreI.setInvoiced(dbObject.getString("invoiced"));
//            gStoreI.setMakedate(dbObject.getString("makedate"));
//            gStoreI.setMakedateo(dbObject.getString("makedateo"));
//            gStoreI.setMakeno(dbObject.getString("makeno"));
//            gStoreI.setMakenoo(dbObject.getString("makenoo"));
//            gStoreI.setMaxqty(dbObject.getString("maxqty"));
//            gStoreI.setMidqty(dbObject.getString("midqty"));
//            gStoreI.setPayed(dbObject.getString("payed"));
//            gStoreI.setPayeetype(dbObject.getString("payeetype"));
//            gStoreI.setPaytime(dbObject.getString("paytime"));
//            gStoreI.setPurprice(dbObject.getString("purprice"));
//            gStoreI.setPurpriceNt(dbObject.getString("purprice_nt"));
//            gStoreI.setPurpriceNto(dbObject.getString("purprice_nto"));
//            gStoreI.setPurpriceo(dbObject.getString("purpriceo"));
//            gStoreI.setPurqty(dbObject.getString("purqty"));
//            gStoreI.setPurtax(dbObject.getString("purtax"));
//            gStoreI.setPurtaxo(dbObject.getString("purtaxo"));
//            gStoreI.setSrcbatchno(dbObject.getString("srcbatchno"));
//            gStoreI.setSrcidno(dbObject.getString("srcidno"));
//            gStoreI.setStamp(dbObject.getString("stamp"));
//            gStoreI.setSupplyno(dbObject.getString("supplyno"));
//
//            list.add(gStoreI);
//        }
        System.out.println("数据读取完成");
        long end = System.currentTimeMillis();
        System.out.println("读取数据库总共耗时：" + (end - start) / 60000 + "min");

        /*******************连接MongoDB*******************/


        /*******************连接Mysql*******************/

        Connection conn = ConnectMySql.getConTest();
        PreparedStatement ps = null;
        String sql = "Insert Into TB_IP_GStoreI (`_id`,backprice,batchno,Bioavailability,buyer,distprice,execdate,flag,flag2,goodscode,idno,initbusno,invalidate,invalidate_char,invalidateo,invoiced,makedate,makedateo,makeno,makenoo,maxqty,midqty,payed,payeetype,paytime,purprice,purprice_nt,purprice_nto,purpriceo,purqty,purtax,purtaxo,srcbatchno,srcidno,stamp,supplyno,ModificationDate)" +
                " Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String del = "Truncate TB_IP_GStoreI";
        int c = 0 ;
        try {
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(del);
            ps.executeUpdate();
            conn.commit();
            System.out.println("已删除历史数据");
            ps = conn.prepareStatement(sql);
            long startTime = System.currentTimeMillis();
//             字段过多，数据量也多的情况下，实体类存储容易内存溢出，可选择边读取边插入的方式进行。
//            for(int i = 0 ;i<list.size();i++) {
//                TbIpGStoreI gStoreI = list.get(i);
//                ps.setString(1, gStoreI.getId());
//                ps.setString(2, gStoreI.getBackprice());
//                ps.setString(3, gStoreI.getBatchno());
//                ps.setString(4, gStoreI.getBioavailability());
//                ps.setString(5, gStoreI.getBuyer());
//                ps.setString(6, gStoreI.getDistprice());
//                ps.setString(7, gStoreI.getExecdate());
//                ps.setString(8, gStoreI.getFlag());
//                ps.setString(9, gStoreI.getFlag2());
//                ps.setString(10, gStoreI.getGoodscode());
//                ps.setString(11, gStoreI.getIdno());
//                ps.setString(12, gStoreI.getInitbusno());
//                ps.setString(13, gStoreI.getInvalidate());
//                ps.setString(14, gStoreI.getInvalidateChar());
//                ps.setString(15, gStoreI.getInvalidateo());
//                ps.setString(16, gStoreI.getInvoiced());
//                ps.setString(17, gStoreI.getMakedate());
//                ps.setString(18, gStoreI.getMakedateo());
//                ps.setString(19, gStoreI.getMakeno());
//                ps.setString(20, gStoreI.getMakenoo());
//                ps.setString(21, gStoreI.getMaxqty());
//                ps.setString(22, gStoreI.getMidqty());
//                ps.setString(23, gStoreI.getPayed());
//                ps.setString(24, gStoreI.getPayeetype());
//                ps.setString(25, gStoreI.getPaytime());
//                ps.setString(26, gStoreI.getPurprice());
//                ps.setString(27, gStoreI.getPurpriceNt());
//                ps.setString(28, gStoreI.getPurpriceNto());
//                ps.setString(29, gStoreI.getPurpriceo());
//                ps.setString(30, gStoreI.getPurqty());
//                ps.setString(31, gStoreI.getPurtax());
//                ps.setString(32, gStoreI.getPurtaxo());
//                ps.setString(33, gStoreI.getSrcbatchno());
//                ps.setString(34, gStoreI.getSrcidno());
//                ps.setString(35, gStoreI.getStamp());
//                ps.setString(36, gStoreI.getSupplyno());
            while (cursor.hasNext()) {
                dbObject = (BasicDBObject) cursor.next();
                ps.setString(1, dbObject.getString("_id"));
                ps.setString(2, dbObject.getString("backprice"));
                ps.setString(3, dbObject.getString("batchno"));
                ps.setString(4, dbObject.getString("Bioavailability"));
                ps.setString(5, dbObject.getString("buyer"));
                ps.setString(6, dbObject.getString("distprice"));
                ps.setString(7, Util.DateFormat(dbObject.getString("execdate")));
                ps.setString(8, dbObject.getString("flag"));
                ps.setString(9, dbObject.getString("flag2"));
                ps.setString(10, dbObject.getString("goodscode"));
                ps.setString(11, dbObject.getString("idno"));
                ps.setString(12, dbObject.getString("initbusno"));
                ps.setString(13, Util.DateFormat(dbObject.getString("invalidate")));
                ps.setString(14, dbObject.getString("invalidate_char"));
                ps.setString(15, Util.DateFormat(dbObject.getString("invalidateo")));
                ps.setString(16, dbObject.getString("invoiced"));
                ps.setString(17, Util.DateFormat(dbObject.getString("makedate")));
                ps.setString(18, Util.DateFormat(dbObject.getString("makedateo")));
                ps.setString(19, dbObject.getString("makeno"));
                ps.setString(20, dbObject.getString("makenoo"));
                ps.setString(21, dbObject.getString("maxqty"));
                ps.setString(22, dbObject.getString("midqty"));
                ps.setString(23, dbObject.getString("payed"));
                ps.setString(24, dbObject.getString("payeetype"));
                ps.setString(25, Util.DateFormat(dbObject.getString("paytime")));
                ps.setString(26, dbObject.getString("purprice"));
                ps.setString(27, dbObject.getString("purprice_nt"));
                ps.setString(28, dbObject.getString("purprice_nto"));
                ps.setString(29, dbObject.getString("purpriceo"));
                ps.setString(30, dbObject.getString("purqty"));
                ps.setString(31, dbObject.getString("purtax"));
                ps.setString(32, dbObject.getString("purtaxo"));
                ps.setString(33, dbObject.getString("srcbatchno"));
                ps.setString(34, dbObject.getString("srcidno"));
                ps.setString(35, dbObject.getString("stamp"));
                ps.setString(36, dbObject.getString("supplyno"));

                ps.setString(37, sdfM.format(new Date()));


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

            System.out.println("成功将"+cursor.count()+"条数据写入到数据库中");
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
