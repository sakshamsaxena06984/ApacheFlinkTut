package com.stream.fraud_detection_bank_casestudy;

public class AlarmedCustomer {
    public final String id;
    public final String account;

    public AlarmedCustomer(){
        id="";
        account="";
    }

    public AlarmedCustomer(String s){
        String[] split= s.split(",");
        id = split[0];
        account = split[1];
    }

}
