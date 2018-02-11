package kafkaconsumer;

import java.io.Serializable;
import java.util.ArrayList;

public class LogEntry implements Serializable {

    public ArrayList<String> ipAddress;
    public Integer flag;

    public LogEntry(int fact, ArrayList<String> ipAddresses){
        this.flag = fact;
        this.ipAddress = ipAddresses;
    }
    public LogEntry() {
        ipAddress = new ArrayList<String>();
        flag = 0;

    }

    public void setFlag() {
        flag = 1;
    }

    public Integer isAttackMode(){
        return flag;
    }
    public ArrayList<String> getIpAddress(){
        return ipAddress;
    }
    public boolean setIpAddress(String value){
        return ipAddress.add(value);
    }
    public boolean containsIpAddress(String someIp){
        return ipAddress.contains(someIp);
    }
    public boolean isListEmpty()
    {
        return ipAddress.isEmpty();
    }
    public Integer getSize(){
        return ipAddress.size();
    }

}
