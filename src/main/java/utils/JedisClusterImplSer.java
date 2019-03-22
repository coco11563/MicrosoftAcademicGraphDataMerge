package utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class JedisClusterImplSer implements Serializable{
    private static final long serialVersionUID = -51L;

    private HostAndPort hostAndPort;
    transient private JedisCluster jedisCluster;

    public JedisClusterImplSer(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
        this.jedisCluster = new JedisCluster(hostAndPort);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(hostAndPort);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        in.defaultReadObject();
        setJedisCluster(new JedisCluster(hostAndPort));
    }

    private void readObjectNoData() throws ObjectStreamException {

    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    private void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }
}
