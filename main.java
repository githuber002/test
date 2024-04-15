package com.tophant
 
import java.text.SimpleDateFormat
 
import com.tophant.html.Util_html
import com.tophant.util.Util
import org.apache.commons.lang.{StringEscapeUtils, StringUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
 
object Hbase2hdfs {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    // 命令行参数个数必须为2
    if (args.length != 5) {
      logger.error("参数个数错误")
      logger.error("Usage: Classify <开始日期> <结束日期> <存储位置> <host>")
      System.exit(1)
    }
 
    // 获取命令行参数中的行为数据起止日期
    val tableName = args(0).trim  //hbase表的前缀
    val startDate = args(1).trim  //hbase表的后缀起始日期
    val endDate   = args(2).trim  //hbase表的后缀截至日期
    val path = args(3).trim
    val host = args(4).trim
    // 根据起止日志获取日期列表
    // 例如起止时间为20160118,20160120,那么日期列表为(20160118,20160119,20160120)
    val dateSet = getDateSet(startDate, endDate)
 
    val sparkConf = new SparkConf()
      .setAppName("hbase2hdfs")
    //      .set("spark.default.parallelism","12")
    val sc = new SparkContext(sparkConf)
 
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "10.0.71.501,10.0.71.502,10.0.71.503")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
 
    //组装scan语句
    val scan=new Scan()
    scan.setCacheBlocks(false)
    scan.addFamily(Bytes.toBytes("rd"))
    scan.addColumn(Bytes.toBytes("rd"), Bytes.toBytes("host"))
    scan.addColumn("rd".getBytes, "url".getBytes)
    scan.addColumn("rd".getBytes, "http.request.method".getBytes)
    scan.addColumn("rd".getBytes, "http.request.header".getBytes)
 
 
    val hostFilter = new SingleColumnValueFilter(Bytes.toBytes("rd"),
      Bytes.toBytes("host"),
      CompareOp.EQUAL,
      Bytes.toBytes(host))
    scan.setFilter(hostFilter)
    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    import org.apache.hadoop.hbase.protobuf.ProtobufUtil
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray)
 
 
    // 按照日期列表读出多个RDD存在一个Set中,再用SparkContext.union()合并成一个RDD
    var rddSet: Set[RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] ] = Set()
    dateSet.foreach(date => {
      conf.set(TableInputFormat.SCAN, ScanToString)
      conf.set(TableInputFormat.INPUT_TABLE, tableName + date) // 设置表名
      val bRdd: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] =
        sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
      rddSet += bRdd
    })
 
    val hBaseRDD = sc.union(rddSet.toSeq)
 
    val kvRDD = hBaseRDD.
      map { case (_, result) =>
        //通过列族和列名获取列
        var url = Bytes.toString(result.getValue("rd".getBytes, "url".getBytes))
        val host = Bytes.toString(result.getValue("rd".getBytes, "host".getBytes))
        var httpRequestHeaders = Bytes.toString(result.getValue("rd".getBytes, "http.request.header".getBytes))
        var httpResponseHeaders = Bytes.toString(result.getValue("rd".getBytes, "http.response.header".getBytes))
 
  
 
        val value = (url + "$$$" + httpRequestHeaders + "$$$" + httpResponseHeaders )
 
        ( host, value)
      }
    .saveAsTextFile(path)
  }
 
  def getDateSet(sDate:String, eDate:String): Set[String] = {
    // 定义要生成的日期列表
    var dateSet: Set[String] = Set()
 
    // 定义日期格式
    val sdf = new SimpleDateFormat("yyyyMMdd")
 
    // 按照上边定义的日期格式将起止时间转化成毫秒数
    val sDate_ms = sdf.parse(sDate).getTime
    val eDate_ms = sdf.parse(eDate).getTime
 
    // 计算一天的毫秒数用于后续迭代
    val day_ms = 24*60*60*1000
 
    // 循环生成日期列表
    var tm = sDate_ms
    while (tm <= eDate_ms) {
      val dateStr = sdf.format(tm)
      dateSet += dateStr
      tm = tm + day_ms
    }
 
    // 日期列表作为返回
    dateSet
  }
}
