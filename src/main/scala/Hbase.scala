//import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
//import org.apache.hadoop.hbase.client.HBaseAdmin
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.spark._
//import org.apache.hadoop.hbase.util.Bytes
//
//
//object Hbase {
//
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local") //初始化
//    val sc = new SparkContext(sparkConf)
//
//    val tablename = "grande"
//    val conf = HBaseConfiguration.create() //连接设置
//    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
//    conf.set("hbase.zookeeper.quorum","192.168.48.101,192.168.48.102,192.168.48.103,192.168.48.104")
//    //设置zookeeper连接端口，默认2181
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set(TableInputFormat.INPUT_TABLE, tablename)
//
//    // 如果表不存在则创建表
//    val admin = new HBaseAdmin(conf)
//    if (!admin.isTableAvailable(tablename)) {
//      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
//      admin.createTable(tableDesc)
//    }
//
//    //读取数据并转化成rdd
//    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//
//    val count = hBaseRDD.count()
//    println(count)
//    hBaseRDD.foreach{case (_,result) =>{
//      //获取行键
//      val key = Bytes.toString(result.getRow)
//      //通过列族和列名获取列
//      val name = Bytes.toString(result.getValue("cf1".getBytes,"name".getBytes))
//      val gender = Bytes.toString(result.getValue("cf1".getBytes,"gengder".getBytes))
//      val chinese = Bytes.toInt(result.getValue("cf2".getBytes,"chinese".getBytes))
//      println("Row key:"+key+" Name:"+name)
//    }}
//
//    sc.stop()
//    admin.close()
//  }
//
//}