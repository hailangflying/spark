import org.apache.spark.{SparkConf, SparkContext}

object FilterApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local") //生成配置实例
    val sc = new SparkContext(conf) //生成 SparkContext 实例

    val textRDD=sc.textFile("/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/Filter.txt",2)

    val filteredRDD =textRDD.filter(_.split(" ")(0).equals("1"))
    val resultRDD=filteredRDD.map(word=> {
      val columns =word.split(" ")
      val phoneNum = columns(1)
      val host=columns(2).replaceAll("/ .*","")
      phoneNum + " " + host
    })

  // resultRDD.saveAsTextFile ("/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/4GAccessLog")







    val textRDD2 =sc.textFile("/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/4GAccessLog")
   // textRDD2 = resultRDD
    val mappedRDD=resultRDD.map(word2 =>{
       val columns = word2.split(" ")
       val propertyl =columns (0)
       val property2 =columns (1)
      //下面对内容是返回值
       (property2 , propertyl )
      })

    val distinctedRDD =mappedRDD.distinct()
    val setOneRDD =distinctedRDD.mapValues (propertyl => 1)
    val reducedRDD = setOneRDD .reduceByKey (_+_)

    reducedRDD.map(x=>x._1 + " "+x._2).saveAsTextFile("/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/dist01")


  }
}
