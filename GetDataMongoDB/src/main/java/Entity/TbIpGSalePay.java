package Entity;

public class TbIpGSalePay {
    private String id ;
    private String cardno;
    private String netsum;
    private String paytype;
    private String saleno;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCardno() {
        return cardno;
    }

    public void setCardno(String cardno) {
        this.cardno = cardno;
    }

    public String getNetsum() {
        return netsum;
    }

    public void setNetsum(String netsum) {
        this.netsum = netsum;
    }

    public String getPaytype() {
        return paytype;
    }

    public void setPaytype(String paytype) {
        this.paytype = paytype;
    }

    public String getSaleno() {
        return saleno;
    }

    public void setSaleno(String saleno) {
        this.saleno = saleno;
    }
}
