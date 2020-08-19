package Entity;


import java.sql.Timestamp;

public class TbIpGStoreC {

  private String id;
  private String awaitqty;
  private String batchno;
  private String busno;
  private String goodscode;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAwaitqty() {
    return awaitqty;
  }

  public void setAwaitqty(String awaitqty) {
    this.awaitqty = awaitqty;
  }

  public String getBatchno() {
    return batchno;
  }

  public void setBatchno(String batchno) {
    this.batchno = batchno;
  }

  public String getBusno() {
    return busno;
  }

  public void setBusno(String busno) {
    this.busno = busno;
  }

  public String getGoodscode() {
    return goodscode;
  }

  public void setGoodscode(String goodscode) {
    this.goodscode = goodscode;
  }

  public String getGoodsqty() {
    return goodsqty;
  }

  public void setGoodsqty(String goodsqty) {
    this.goodsqty = goodsqty;
  }

  public String getIdno() {
    return idno;
  }

  public void setIdno(String idno) {
    this.idno = idno;
  }

  public String getStallno() {
    return stallno;
  }

  public void setStallno(String stallno) {
    this.stallno = stallno;
  }

  public String getStamp() {
    return stamp;
  }

  public void setStamp(String stamp) {
    this.stamp = stamp;
  }

  public Timestamp getModificationDate() {
    return modificationDate;
  }

  public void setModificationDate(Timestamp modificationDate) {
    this.modificationDate = modificationDate;
  }

  private String goodsqty;
  private String idno;
  private String stallno;
  private String stamp;
  private java.sql.Timestamp modificationDate;

}