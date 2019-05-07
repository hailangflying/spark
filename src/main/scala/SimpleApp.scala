import org .apache .spark .SparkContext
import org .apache .spark .SparkContext ._
import org .apache .spark .SparkConf
object SimpleApp {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("error : too few arguments")
      //  sys.exit(1)
    }
      val inputFile = args(0) //读取输入文件路径参数
      val outputFile = args(1) //读取输出文件路径参数
      val conf = new SparkConf().setAppName("WordCount").setMaster("local") //生成配置实例
       val sc = new SparkContext(conf) //生成 SparkContext 实例
      val file = sc.textFile(inputFile, 3) //读取输入文件内容生成 RDD
      val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _) //统计
//    counts.cache()
    counts.collect
      counts.saveAsTextFile(outputFile) //保存统计结果

    Thread.sleep(1000000)

  }
}