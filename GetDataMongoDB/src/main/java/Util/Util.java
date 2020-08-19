package Util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Util {
    public static String String2Json(String s)
    {
        StringBuilder sb = new StringBuilder();
        if (s == null || s.equals("")){

        }
        else {

            for (int i = 0; i < s.length(); i++)
            {
                char c = s.toCharArray()[i];
                switch (c)
                {
                    case '\"':
                        sb.append("\\\""); break;
                    case '\\':
                        sb.append("\\\\"); break;
                    case '/':
                        sb.append("\\/"); break;
                    case '\b':
                        sb.append("\\b"); break;
                    case '\f':
                        sb.append("\\f"); break;
                    case '\n':
                        sb.append("\\n"); break;
                    case '\r':
                        sb.append("\\r"); break;
                    case '\t':
                        sb.append("\\t"); break;
                    default:
                        if ((c >= 0 && c <= 31)||c ==127)//在ASCⅡ码中，第0～31号及第127号(共33个)是控制字符或通讯专用字符
                        {

                        }
                        else
                        {
                            sb.append(c);
                        }
                        break;
                }
            }
            return sb.toString();

        }
        return  s;
    }


    public static String IFNULL2Json(String f){

        if(f!= null) {
            f="\""+f+"\"";
        }
        else {
            f="null";
        }
        return  f;
    }



    public static String DateFormat(String date)  {


        SimpleDateFormat sdfG = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy",Locale.US);
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (date == null || date.equals("")){
            return date;
        }

        if(date.contains("G") || date.contains("C")){
            Date dateG = null;
            try {
                dateG = sdfG.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            date=sdf.format(dateG);

            return date;

        }else {

            Date dateT = null ;
            date=date.replace("/","-").replace(".","").trim();
            if (date.length()<19){
                date=date+" 00:00:00";            //处理精度短的日期
            }

            try {
                dateT=sdf.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            date=sdf.format(dateT);

            return date;
        }
    }




    public static void  main(String[] args){
//        String s=Util.DateFormat("Wed Sep 16 19:02:36 CST 2012");
//        String s=Util.DateFormat("1582186182617");
//        System.out.println("s="+s);

//        String s=Util.IFNULL2Json(Util.String2Json("1\\2"));
//        System.out.println("s="+s);
//        System.out.println("123456789\\");
        String hdfs_path = "hdfs://10.254.2.143:9000/test/t_dept.json";
        String Items=null;

//        try {
//            int result = readHdfsTest(hdfs_path);
//
//            System.out.println("Look at what this is -->  "+result);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
