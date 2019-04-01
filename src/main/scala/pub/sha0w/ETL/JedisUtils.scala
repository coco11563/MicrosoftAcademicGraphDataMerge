package pub.sha0w.ETL

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}
import utils.JedisImplSer

import scala.collection.JavaConversions._
object JedisUtils {
        def buildRedisClusterConf(redisHost : String, redisPort : String) : util.Set[HostAndPort] = {
          var setRet = new util.HashSet[HostAndPort]
          val hosts = redisHost.trim.split(",")
          val ports = redisHost.trim.split(",")
          for (h <- hosts) {
            for (p <- ports) {
              setRet.add(new HostAndPort(h, java.lang.Integer.valueOf(p)))
            }
          }
          setRet
        }
        def buildRedisConf(redisHost : String, redisPort : String) : HostAndPort = {
          new HostAndPort(redisHost, java.lang.Integer.valueOf(redisPort))
        }


        def resetRedis(jedisCluster: JedisCluster): Unit = {
          import scala.collection.JavaConversions._
          for (pool <- jedisCluster.getClusterNodes.values) {
            val jedis = pool.getResource
            try
              jedis.flushAll
            catch {
        case ex: Exception =>
          System.out.println(ex.getMessage)
      } finally if (jedis != null) jedis.close()
    }
  }

  def resetRedis(jedis: Jedis): Unit = {
    try
      jedis.flushAll
    catch {
      case ex: Exception =>
        System.out.println(ex.getMessage)
    } finally if (jedis != null) jedis.close()
  }

  def recordDuplicate(rows: Iterable[Row], primaryKey : String, jedisCluster: JedisCluster) :Row = {
    var f = rows.head //rows length >= 1
    if (rows.size < 2) return f //only one elem
    for (row <- rows) {
      jedisCluster.set(row.getAs[String](primaryKey) , row.getAs[String](primaryKey)) //set son -> father(1 -> 1)
    }
    f
  }

  // new
  // 127.0.0.1:6379> hget 0b78f337788d450f86e8530f63d8d9d3hset fruit
  // "0b78f337788d450f86e8530f63d8d9d3|\xe4\xbd\x8e\xe4\xbe\xa7\xe6\xb5\x81\xe7\x83\x9f\xe6\xb0\x94\xe5\x8d\xb7\xe7\x83\x9f"
  // test pass
  def keyFieldPersist(df : DataFrame, jedisImplSer: JedisImplSer, fieldName : String
                      , idName :String, field : String) : Unit = {
    val schema = df.schema
    println(s"schema is $schema")
    val index = schema.fieldIndex(field)
    println(s"$field's index is $index")
    val idIndex = schema.fieldIndex(idName)
    println(s"$idName's index is $idIndex")
    val rdd = df.rdd
    rdd
      .map(r => (r.getAs[String](idIndex), r.getAs[String](index)))
      .foreach(str => {
        jedisImplSer.getJedis.hset(str._1 + "hset", fieldName ,str._1 + "|" + str._2)
      })
  }
  // new
  def relationPersist(df : DataFrame, jedisImplSer: JedisImplSer, fieldName : String) : Unit = {
    df.rdd
      .foreach(r => {
        val key = r.getAs[String](0)
        val value = r.getAs[String](1)
        val map = jedisImplSer.getJedis.hgetAll(key + "hmset")
        if (map.isEmpty) jedisImplSer.getJedis.hmset(key+ "hmset", Map[String,String](fieldName-> value))
        else {
          val old = map.get(fieldName)
          if (old.isEmpty) {
            map.put(fieldName, value)
          } else {
            map.put(fieldName, value + "," + old)
          }
          jedisImplSer.getJedis.hmset(key + "hmset", map)
        }
      })
  }
  // new
  def getKeyField(jedisImplSer: JedisImplSer, key : String, field : String) : String = {
    val keys = Option(jedisImplSer.getJedis.hmget(key + "hmset", field)(0))
    if (keys.isEmpty) null
    else {
      val list = keys.get.split(",")
      list.map(str => jedisImplSer.getJedis.hget(str + "hset", field)).reduce(_ + "," + _)
    }
  }

  def getFather(key:String, jedis : JedisImplSer): String = {
    val s = jedis.getJedis.get(key)
    if (s == null || s == key) jedis.getJedis.get(key)
    //not son of any return itself
    // I know you will feel confuse , just relax :-)
    else getFather(s, jedis)
  }

  def getOneLayerFather(key:String ,jedis : Jedis): String = {
    jedis.get(key)
  }
}
