
package gobblin.util;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import java.io.*; 
import java.net.*;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
  

/**
 *
 * @author Ali Rakhshanfar
 */
public class PGStatsClient {
    private String host;
    private int port;
    private String source;

    private HashMap<String, Double> queue;
    private boolean enabled;
    private static final Logger LOG = LoggerFactory.getLogger(PGStatsClient.class);

    public PGStatsClient(String host, String port, String enabled){
        this.host = host;
        this.port = Integer.parseInt(port);
        if(enabled.equals("true")){
            this.enabled = true;
        }else{
            this.enabled = false;
        }
        this.queue = new HashMap<String, Double>();
        this.source = "gobblin";
    }

    public void flush(){
        ArrayList<String> metrics = new ArrayList<String>();
        for (Map.Entry<String, Double> entry : queue.entrySet()){
            LOG.info("Metrics "+String.format("%s:%s|c", entry.getKey(), entry.getValue()));
            metrics.add(String.format("%s:%s|c", entry.getKey(), entry.getValue()));
        }
        if(!metrics.isEmpty()){
            pushData(StringUtils.join(metrics, "\n"));
        }
        this.queue = new HashMap<String, Double>();
    }
    private void pushData(String data){
        LOG.info("Metrics are " + enabled + "Host: " + host + "Port: " + port);
        if(!enabled){
            return;
        }
        try{
            InetAddress IP = InetAddress.getByName(host);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData = new byte[1024];
            sendData = data.getBytes("UTF-8");
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IP, port);       
            clientSocket.send(sendPacket);       
            clientSocket.close();
        } catch(java.net.SocketException e){
            LOG.error("SocketException while sending stats.");
        } catch(java.io.IOException e2){
            LOG.error("IOException while sending stats.");
        }
    }
    public void pushCounter(String metric, double value){
        String key = String.format("%s.%s", this.source, metric);
        if(queue.containsKey(key)){
            value += queue.get(key);
        }
        queue.put(key, value);
    }
    public void pushCounter(String metric, int value){
        pushCounter(metric, (double) value);
    }
}
