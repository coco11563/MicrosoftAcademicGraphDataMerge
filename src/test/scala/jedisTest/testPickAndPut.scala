package jedisTest

import pub.sha0w.ETL.util.JedisUtils
import redis.clients.jedis.HostAndPort
import utils.JedisImplSer

object testPickAndPut {
  def main(args: Array[String]): Unit = {
    val jedis : JedisImplSer = new JedisImplSer(new HostAndPort("10.0.88.50", 6379))
    JedisUtils.resetRedis(jedis.getJedis)
//
//    for (i <- 0 to 100000000) {
//      val uuid = UUID.randomUUID()
//      jedis.getJedis.set("test_key" ,"test_value")
//      var test_key = uuid.toString + "test_key"
//      if (i % 100000 == 0) {
//        println(i + ":")
//        val start = System.currentTimeMillis()
        //val ens = jedis.getJedis.get("test_key")
//    println(ens)
//        println(ens)
//        val stop = System.currentTimeMillis()
//        println("ending in : " + (stop - start))
//      }
//    }
  }
}
