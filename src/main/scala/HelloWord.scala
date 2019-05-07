import org.apache.spark.{SparkConf, SparkContext}

object HelloWord {


  def main(args: Array[String]): Unit = {

    val logFile = "/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/hello.txt"
    val conf = new SparkConf().setAppName("helloword").setMaster("local");
    val sc = new SparkContext(conf);
    val logData = sc.textFile(logFile);
    logData.collect
    val lines = logData.flatMap(line=>line.split(" "));
    val count = lines.map(word=>(word,1)).reduceByKey((x,y)=>(x+y));
    println("==============")
   // count.saveAsTextFile("/Users/niuniu/doc/ziroom_doc/learn/helloword/test-01/hello_out.txt")
    println(count)




    var a = 0;
    val numList = List(1,2,3,4,5,6,7,8,9,10);

    // for loop execution with a yield
    var retVal = for{ a <- numList if a != 3; if a < 8 }yield a

    // Now print returned values using another loop.
    for( a <- retVal){
      println( "Value of a: " + a );
    }



//makeRDD
    val rdd = sc.makeRDD(1 to 6,2);
    rdd.collect
    val data =Seq((1 to 6, Seq("hostl","host2")), (7 to 10, Seq
    ("host3")))
    val rdd1 = sc.makeRDD (data)
    rdd1.collect
    rdd1.preferredLocations (rdd1.partitions (0))

    rdd1.preferredLocations (rdd1.partitions (1))


/*
    val rdd=sc.parallelize(1 to 6,2)
    rdd.collect
    rdd.partitions

    val rdd = sc .makeRDD (data)

    rdd.preferredLocations (rdd .partitions (0))
    rdd.preferredLocations (rdd .partitions (1))




    val textFile=sc.textFile(呗EADME.rnd”)//读取README.rnd文件 textFile: spark .RDD[String] =spark .MappedRDD@ 2ee9b6e3
    textFile.count() 11显示 rdd 的行数
    textFile.first() //显示 rdd 中第一行的内容 r esl : String =#Apache Spark




    //加载整个目录下的
    val rdd =sc.wholeTextFiles (”hdfs ://data ")
    textFile.count()//显示 rdd 的行数 resO : Long =126
    textFile.first()//显示 rdd 中第一行的内容 resl : String =# Apache Spark


//变换算子

 //map   map 算子也 是将函数 f作用于当前 RDD 的每个元素，形成一个新的 RDD。
 val list=sc.parallelize(List(1,2,3,4),2)//构建一个名为 list 的 RDD
 list.map(x=>x+1).collect//对list中的每个元素进行+1操作，形成 新的 RDD



//重新分区将 当前 RDD 进行重新分区，生成一个以 numPartitions
//参数指定的分区数存储的新 RDD。 参数 shuffle 为 true 时在变换过程中进行 shuffle 操作，否则不进行 shuffle。

   val rdd =sc .parallelize (List (工，2,3 ,4,5,6,7 ,8) ,4 ) //构建一个 4 个 分区的 RDD
   rdd .partitions .length //显示 rdd 的分区数量
   rdd.glom.collect   //使用glom变换显示rdd存放在4个分区中的数据
   val newRDD =rdd.coalesce (2, false )//将 rdd 变换为只有 2 个分区的 新 RDD
   newRDD.partitions.length //显示 newRdd 的分区数 res2 : Int =2

   newRDD.glom.collect //使用 glom 变换显示 newRDD 存放在 2 个分区中的 数据



//distinct一一去重
//-d工stinct () : JavaRDD [T]
//-distinct (numPartitions: Int) : JavaRDD [T]   返回原 RDD 中所有元素去重之后的结果，即原 RDD 中每个元素在新生成的 RDD 中只出现一次，且新 RDD 的分区数等于 numPartitions。

val rdd = sc.parallelize (List (1,1,1 ,1,2 ,2 ,2 ,3 ,3 ,4) ,2) //生成由数 字构成的 RDD
rdd.distinct.collect //显示 rdd去重后的结果


//filter一一过滤
- filter (f: Function [T, Boolean]): JavaRDD [T]
原 RDD 中的所有元素，通过输入参数 f指定的过滤函数进行判别，使过滤函数返 因为 true 的所有元素构成一个新的 RDD

val rdd = sc.parallelize (0 to 9 ,2) //生成由数字 0 9 序列构成的
val filteredRDD=rdd.filter（_%2==0)//从rdd中挑出能被2整除


//flatMap·一-flatMap 变换
 map 变换是对原 RDD 中的每个元素进行一对一变换生成 新 RDD ，
 flatMap 不同的地方在于，它是对原 RDD 中的每个元素用指定函数
 f进行一 对多(这也是 flat前缀的由来)的变换，然后将转换后的结果汇聚生成新 RDD。


  val rdd = sc.parallelize (0 to 3 ,1) //生成由 0 -3 序列构成的 RDD
  val flatMappedRDD=rdd.flatMap(x=>O to x) //使用 flatMap 将每个 原始变换为一个序列
  flatMappedRDD.collect //显示新的 RDD


//pipe一一调用 Shell 命令

 val rdd=sc.parallelize(0 to 7,4)//生成由0-9的序列构成的RDD，存 放在 5 个分区中
 rdd.glom.collect//显示每个分区的数据
 rdd.pipe("head -n 1”).collect//提取每个分区的第1个元素生成新 的 RDD

//sample一一抽样
//对原始 RDD 中的元素进行随机抽样，抽样后产生的元素集合构成新的 RDD。
//参 数 fraction 指定新集合中元素的数量占原始集合的比例 。 抽样时的随机数种子由 seed 指定。
//参数 withReplacement 为 false 时，抽样方 式为不放回抽样 。 参数 withReplacement 为 true 时，抽样方式为放回抽样 。


val rdd = sc.parallelize (0 to 9 ,1) //生成由 0 - 9 的序列构成的 RDD rdd: org. ~pache. spark . rdd. RDD [工口t] = ParallelCollectionRDD [5] at
rdd.sample (false,O.5).collect //不放 回抽样 一 半比例的元素生成新 的 RDD
rdd.sample (false ,0.5).collect //再次不放回抽样一半比例的元素生成新的 RDD res7 : Array [Int] =Array (0 ，工， 3' 6' 8)
rdd.sample (false ,0.8).collect //不放回抽样 80毡比例的元素生成新的
rdd.sample (true,0.5).collect //放回抽样一半比例的元素生成新的 RDD



sortBy一一排序
//对原 RDD 中的元素按照函数 f指定的规则进行排序，并可使用参数 ascending 指 定按照升序或者降序进行排列，排序后的结果生成新 的 RDD ，新 RDD 的分 区数量可以
//由参数 numPartitions 指定，默认采用与原 RDD 相同的分 区数 。


val rdd=sc.parallelize(List(2,1,4,3),1)//生成原始 RDD
rdd.sortBy(x => x,true).collect     //对原始 RDD 中的元素进行升序排列 后的结果
rdd.sortBy(x=>x,false).collect    //对原始 RDD 中的元素进行降序排 列后的结果



//对两个Value型RDD进行变换

//cartesian一一笛卡尔积
//输入参数为另一个 RDD，返回两个 RDD 中所有元素的笛卡尔积，即生成由当前 RDD 的所有元素与输入参数 RDD 的所有元素两两组合构成的所有可能的有序对 集合。


val rdd_1 = sc.parallelize (List (”a ”,”b ”,”c") ,l)
val rdd_2 = sc.parallelize (List (1,2 ,3) ,1)
rdd_1.cartesian (rdd_2).collect


//intersection一一交集
val rdr_1 = sc.parallelize (List (1，2,3,4) ,1 )//构建原始 RDD
val rdd_2 = sc.parallelize (List (2 ,3 ,4 ,5) ,1) //构建输入参数 RDD
rdd_1.intersection(rdd_2 ,1).collect //求两个 RDD 的交集


//subtract-一补集
val rdd_1= sc.parallelize(0 to5,1)
val rdd_2= sc.parallelize(0 to 2 ,1)
rdd_1.subtract(rdd_2).collect



//union一一并集
val rdd_1 = sc.parallelize(0 to 5 ,1) //构建原始 ROD
val rdd_2 = sc.parallelize (4 to 6 ,1 ) //构建输入参数 ROD
rdd_1.union(rdd_2).collect //生成两个 ROD 的并集
rdd_1.union(rdd_2).distinct().collect //生成两个 ROD 的并集并 去重


//zip一一联结
//输入参数为另一个 RDD,zip 变换生成由原始 RDD 的值为 key、输入参数 RDD 的 值为 Value 依次配对构成的所有 Keyl Value 对，并返回这些 Keyl Value 对集合构成的 新 RDD。
//顾名思义就是像拉链一样彼此咬合生成新的集合,长短不一的时候以短为准
val rdd_1=sc.parallelize(0 to 4,1) //构建原始RDD
val rdd_2=sc.parallelize(5 to 9 ,1) //构建输入参数 RDD
rdd_1.zip(rdd_2).collect //对两个 RDD 进行联络



//对 Key/ Value 型 ROD 进行变换
//使用 map 创建 Key/ Value 型 RDD

val words = sc.parallelize(List("apple","banana","berry", "cherry","cumquat","haw"),1)//构建原始 RDD
words.collect //显示原始 RDD
val pairs =words.map(x =>(x(0),x)) //使用 map 变换生成 Key/Value 型 RDD
pairs.collect //显示生成的 Key/Value 型 RDD



//使用 keyBy 创建 Key/ Value 型 RDD
val words = sc.parallelize(List("apple","banana","berry", "cherry","curnquat”，"haw"),1) //构建原始 ROD
words.collect //显示原始 ROD
val pairs=words.keyBy(_.length) //使用 keyBy 变换生成 Key/Value 型 ROD
pairs.collect //显示生成的 Key/Value 型 ROD


==========
//对单个 Key-Value 型 RDD 进行变换
//combineByKey一一按 Key 聚合
- combineByKey [C] (createCombiner : Function [V, C] , mergeValue: Function2 [C, V, C], mergeCombiners: Function2 [C, C, C]) : JavaPairRDD [K, C]


// 将 Key/ Value 型的 RDD 按 Key 进行聚合( Combine)计算，该变换的计算过程是将 RDD 中的< Key, V alue >对依次输入该变换，然后使用输入参数中的 3 个函数进行如 下计算。

 createCombiner:在遍历 RDD 的一个分区时，对于依次处理的每个< Key, V alue> 对，如果 Key 是第一次出现，则触发执行 createCombiner 函数，将< Key, Value> 中的 Value 转换由函数指定的数据类型 C。
 createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
 mergeValue:对于依次处理的每个< Key, value 〉对，如果 Key 不是第一次出 现，则触发执行 mergeValue 函数，将前一次计算获得的
 C(可能是第一次 Key 到 达时由 createCombiner 函数生成，也可能是后续 Key 到达时由 mergeValue 函数 生成)
 与当前到达的< Key, value >对中的 Value 用 mergeValue 函数计算，得到 新的 C。
 mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
 mergeCombiners:  Spark 是一个分布式计算环境，在执行聚合变换 时，RDD 可能分布在不同的计算节点进行变换 。 因此，要获得整体数据的聚合 结果，最后还需要将分散的相同 Key 值的数据汇集到一起运算，这就是 mergeCombiners 的作用 。 在前面两个函数计算获得的所有的 C，都要经过 mergeCombiners 汇聚成最终结果的 C。
mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)


val pair = sc.parallelize (List (("fruit","Apple"),("fruit", "Banana"),("vegetable","Cucumber"),("fruit","Cherry"),
("vegetable","Bean"), ("vegetable","Pepper")) ,2) //生成原始 RDD
val combinedPair =pair.combineByKey(List(_), (x:List[String],y:String) =>y :: x, (x:List[String]), y:List[String))=> x ::: y) //进 CombineByKey 变换

combinedPair.collect //输出变换结果
resO: Array [(String, List [String]))= Array ((fruit, List (Cherry, Banana, Apple)), (vegetable,List (Pepper, Cucumber, Bean)))
//示例代码中，输入 RDD 为 Key/ Value 型的 RDD,Key 为物品的类型名称，例如水 果 (fruit)或蔬菜( vegetable ) , Value 为具体的物品名称，例如苹果( Apple)或香蕉
(Banana) 。 示例代码的功能是将属于同一类的物品汇聚到一起形成一个 List。 其关键 在于第 2 行代码中指定的 3 个函数:
-指定的 createCombiner 函数为 List(一)，其作用是在某类物品的第一个物品出现 时，将该物品的名称放入一个 List 中，例如< fruit,Apple >到达时会生成 List(Apple)。
-指定的 mergeValue 函数为( x:List[String], y:Str{ng) = > y : : x，其作用是将不 是第一次出 现的 类型 的物品名称，放人已生成的 List 中，例如< fruit, Banana 〉到达时 会生成List(Apple, Banana)。
-指定的 mergeCombiners 函数为( x: List[ String] , y: List[ String]) = > x : : : y，其 作用是将前两个函数生成的某物品类别的所有列表合并为一个列表。，


//qiu

val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
val d1 = sc.parallelize(initialScores)
type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
d1.combineByKey(
  score => (1, score),
  (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
  (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
).map { case (name, (num, socre)) => (name, socre / num) }.collect



/**
*
* 参数含义的解释
a 、score => (1, score)，我们把分数作为参数,并返回了附加的元组类型。 以"Fred"为列，当前其分数为88.0 =>(1,88.0)  1表示当前科目的计数器，此时只有一个科目

b、(c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore)，注意这里的c1就是createCombiner初始化得到的(1,88.0)。在一个分区内，我们又碰到了"Fred"的一个新的分数91.0。当然我们要把之前的科目分数和当前的分数加起来即c1._2 + newScore,然后把科目计算器加1即c1._1 + 1

c、 (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)，注意"Fred"可能是个学霸,他选修的科目可能过多而分散在不同的分区中。所有的分区都进行mergeValue后,接下来就是对分区间进行合并了,分区间科目数和科目数相加分数和分数相加就得到了总分和总科目数

* */






//flatMapValues一一对所有 Value 进行 flatMap
//flatMapValues类似于mapValues，不同的在于flatMapValues应用于元素为KV对的RDD中Value。
//每个一元素的Value被输入函数映射为一系列的值，然后这些值再与原RDD中的Key组成一系列新的KV对。

val rdd = sc.parallelize(List ("a","boy"),1).keyBy(_.length)
rdd.collect
rdd.flatMapValues(x=>"* "+x+"* ").collect

//解释：先把每个value 变成 *value*，在对value进行映射为值




//groupByKey-一按 Key 汇聚
- groupByKey () : JavaPairRDD [K, Iterable [VJ J

将 Key/ Value型 RDD 中的元素按 Key值进行汇聚，Key值相同的 Value值合并在 一个序列中，所有 Key值的序列构成新的 RDD。需要注意的是，groupByKey变换对每 个分区进行操作后的输出不会进行合井，因此整个变换的开销 是非常大的，应该尽量 避免使用，尽量使用可以完成类似运算的 aggregateByKey或 reduceByKey。 gro叩upByKey 变换还有两个变型 ，分别为可以指定分区数量 的 groupByKey( n
以指定分 区类 的 groupByKey(partitioner: Partitioner) 。


val pairs =sc.parallelize(List(("fruit","Apple"), ("vegetable","Cucumber")("fruit","Cherry"), ("vegetable",”Bean"),
 ("fruit"，"Banana")， ("vegetable"，"Pepper") ),2) //生成原始 RDD
pairs.groupByKey.collect //进行 groupByKey 变换



//keys一一-提取Key

val pairs = sc.parallelize(List ("apple","banana","berry", "cherry"，"cumquat"，"haw") ,1).keyBy(_.length) //构建原始 RDD
pairs.keys.collect //提取单词长度


//mapValues一一对 Value 值进行变换
//将 Key/ Value 型 RDD 中的每个元素的 Value 值，使用输入参数函数 f进行变换，
生成新的 RDD。
val pairs = sc. parallelize (List ("apple","banana","berry", "cherry","cumquat"，"haw") ,1) .keyBy(_.length)//构建原始 RDD
pairs.mapValues(v => v +" "+ v(0)) .collect //生成将单词加单词首字 母的 RDD


//partitionBy一一按 Key 值重新分区
将 Key/ Value 型 RDD 中的元素按照输入参数 partitionBy 指定的分区规则进行重 新分区，生成新的 RDD。


val pairs=sc.parallelize(0 to 9,2).keyBy(x=>x) //构建原始RDD
pairs.glom.collect //显示原始 RDD 的分区情况
import org.apache.spark.HashPartitioner //导人 HashPartitioner 类库 import org .apache .spark .HashPartitioner
val partitionedPairs =pairs.partitionBy(new HashPartitioner(2)) //按照 Key 的 Hash 值进行重新分区
partitionedPairs.glom.collect //显示重新分区后 的结果




//reduceByKey一一按 Key 值进行 Reduce 操作
// 将 Key/ Value 型 RDD 中的元素按 Key 值进行 Reduce 操作， Key 值相同的 Value 值按照参数 func 的逻辑进行归并，然后生成新的 RDD。 需要注意的是， reduceByKey 变 换还有两个变型，分别为可以指定分区数量的 reduceByKey( ( func: Function2 [ V , V , VJ ) , numPartitions: Int)和可以指定分区类的 reduceByKey( partitioner: Partitioner , func : Function2[ V, V, VJ )。

 val fruits = sc. parallelize (List (”apple ”,”banana ”,”berry ”, ”cherry ”,”cumquat ”,”haw”) ,1).keyBy(_.length)
 fruits.reduceByKey (_ +""+ _).collect


//sortByKey一一按 Key 值排序
val words = sc.parallelize (List (”apple ”,”banana ”,”cat ”,”door”), 1).keyBy(_.length) //构建原始 RDD
words.sortByKey(true).collect //按单词长度排序



//values
对 Keyl Value 型的 RDD 进行取值操作，即将 RDD 转化为只有元素的 Value 值构 成的新 RDD。


val words =sc.parallelize(List(”apple”,”banana”,”cat”,”door"), 1) .keyBy (_.length) //构建原始 RDD
words .values .collect //取出 Value 值



=========
//对两个 Key-Value 型 RDD 进行变换
cogroup-一一按 Key 值聚合
对于任何一个在 Keyl Value 型的原始 RDD 或在 Keyl Value 型的输入 RDD(输入 参数 other)中存在的 Key, cogroup 变换会寻找在两个 RDD 中相同 Key 值的元素，将所 有这些元素的 Value 聚合构成一个序列，然后与 Key 值生成新的 RDD。 与后面会讲解 的 join 变换不同之处在于，在新 RDD 中出现的 Key 值并不要求在两个 RDD 中都存在 。

val pair1 =sc.parallelize(List((1,"a"),(2,"b"),(3,"C")),2) //丰句 建原始 RDD
val pair2 =sc.parallelize(List((1,"apple"),(2,"banana")),2) // 构建输入参数 RDD
val cogrouped = pairl.cogroup(pair2, 1).collect //按 Key 值进行 联结



//join一一按Key值联结
//对于在 Keyl Value 型的原始 RDD 和在 Keyl Value 型的输入 RDD (输入参数 other)中同时存在的 Key,join 变换会寻找在两个 RDD 中相同 Key 值的元素，将所有这 些元素的 Value 联结构成一个序列，然后与 Key 值生成新的 RDD。 与前面讲解的 cogroup 变换不同之处在于 ，在新 RDD 中出现的 Key 值要求在两个 RDD 中都存在 。

val pairl =sc.parallelize(List((1,”a”),(2,"b"),(3,"c")),2) //构 建原始 RDD
val pair2 =sc.parallelize(List((1,"apple”),(2,"banana")),2) //构 建输入参数 RDD
pair1.join (pair2,1).collect //按 Key值进行联结



//leftOuterJoin一一按 Key 值进行左外联结

val a = sc.parallelize (List (”a ”,”boy”,”cafe ”),1 ).keyBy(_.length)
a.collect
val b = sc.parallelize(List("dog" , "enjoy" , "fate") ,1).keyBy(_.length)
b.collect
a.leftOuterJoin(b).collect



//rightOuterJoin一一按 Key 值进行右外联结


val a =sc.parallelize(List(”a”,"boy”,”cafe”),1).keyBy(_. length)
a.collect

val b =sc.parallelize(List{”dog”,”enjoy”,”fate"),1).keyBy(_.length)
b.collect
a.rightOuterJoin(b).collect


//subtractByKey一一按 Key 值求补
对于只在 Keyl Value 型的原始 RDD 中出现，而不在 Keyl Value 型的输入 RDD
(输入参数 other)中出现的 Key值，将所有这些 Key值对应的 Keyl Value对元素，生成 新的 RDD。

val words =sc.parallelize(List("apple","banana","cat","door"), 1).keyBy(_.length) //构建原始 RDD
val others=sc.parallelize(List("boy","girl"),1).keyBy(_.length) //构建输入参数 RDD
words.subtractByKey(others).collect //按 Key 值求补






==========
行动算子   行动( Action)算子语句时

//reduce-一-Reduce 操作

//对 RDD 中的每个元素依次使用指定的函数 f进行运算，并输出最终的计算结果 。
//需要注意的是， Spark 中的 reduce 操作与 Hadoop 中的 reduce 操作并不一样 。 在 Hadoop 中，reduce 操作是将指定的函数作用在 Key 值相同的全部元素上 。 而 Spark 的 reduce 操作 则 是对所有元素依次进行相同的函数计算 。

//求和
val nurns = sc.parallelize(0 to 9 ,5) //构建由数字。- 9 构成的 RDD
nums.reduce(_+_)    //计算 RDD 中所有数字的和




//aggregate一一聚合操作
- aggregate[U] (zeroValue: U) (seqOp: Function2 [U, T, U], combOp: Function2 [U, U, U]) : U
//aggregate操作使用参数 seqOp指定的函数对每个分区里面的元素进行聚合，
然后 用参数 combOp 指定的函数将每个分区的聚合结果进行再次聚合，在进行 combOp 聚 合时，
计算的初始值由参数 zeroValue指定。 需要注意的是，聚合操作在进行每个分区 的 seqOp操作以及最终的 combOp操作时，生成结果的数据类型可能与 RDD 中的元素 不一样 。

val rdd = sc.parallelize(List (1 ,2 ,3 ,4 ,5 ,6) , 2) //构建原始 RDD
rdd.glom.collect //显示原始 RDD
rdd.aggregate(0)(_+_,Math.max(_,_))//对原始RDD进行聚合，求每个 分区元素相加后的最大值


//collect-一收集元素
collect 的作用是以数组格式返回 RDD 内的所有元素 。


val data= sc.parallelize (List (1,2 ,3 ,4 ,5 ,6,7 ,8 ,9 ,0) ,2) //构建原 始 ROD
data.collect //显示原始 ROD 中的元素




//collectAsMap一一收集 Key/ Value 型 RDD 中的元 素

val pairRDD = sc.parallelize (List ((1 ,"a") ,(2 ,"b") ,(3 ,"c") ,(4,"d")),2) //构建原始ROD
pairRDD.collectAsMap //以 Map 的方式返回 ROD 中的结果


//count一一计算元素个数
计算井返回 RDD 中元素的个数。

val rdd = sc.parallelize (List (1 ,2 ,3 ,4 ,5 ,6) ,2) //构建原始 ROD
rdd.count //计算 ROD 中元素的个数



//countByKey一一按 Key 值统计 Key/ Value 型 RDD 中的元素个数
- countByKey () : Map [K, Long]
计算 Key/ Value 型 RDD 中每个 Key 值对应的元素个数，并以 Map 数据类型返回 统计结果 。
 注意返回对是数据  不是rdd了

val pairRDD=sc.parallelize(List(("fruit","Apple"),("fruit", "Banana"),("fruit","Cherry"),("vegetable","bean"),("vegetable", ”cucumber")，("vegetable”，"pepper") ) ,2 )//构建原始 ROD
pairRDD.countByKey //统计原始 ROD 中每个物品类型下的物品数量



//countByValue一一统计 RDD 中元素值出现的次数
- countByValue() : Map [T , L o n g ]
计算 RDD 中每个元素的值出现的次数，并以 Map 数据类型返回统计结果 。

val num = sc.parallelize (List (1 ,1,1 ,2 ,2 ,3) ,2) //构建原始 RDD
num.countByValue //统计原始 ROD 中每个数字出现的次数


// first一一-得到首个元素
val words =sc.parallelize (List ("first”，"second"，"third"),1 )
words.first //得到首个元素


//glom一一返回分区情况
//原始 RDD 每个分区中的元素放到一个序列中，并汇集所有分区构成的序列生成 新的 RDD 返回 。

 val num= sc.parallelize (0 to 10 ,4) //构建原始 ROD
 num.glom.collect//显示分区中的元素分布情况



//fold-合并

-fold(zeroValue:T)(f:Function2[T, T, T]):T
将 RDD 中每个分区中的元素使用指定的函数参数 f做合并计算，然后再加所有分 区生成的结果使用 f 函数做合并 。
在分区内和不同分区间进行合并的初始值由 zeroValue 指定 。
//在结果的最前面 有两个 |. 字符 ，是因 为在分区内合并时要由初始值带入一个|.，在所有分区结果合并时，又要带入一个 |. 。


val words=sc.parallelize(List("A","B","C","D"),2) //构建原始 RDD
words.glom.collect //显示原始 RDD 的分区情况
words.fold("|")(_+"."+_)//对原始 RDD执行合并


// foreach一一逐个处理 RDD 元素
对 RDD 中的每个元素，使用参数 f指定的函数进行处理 。

val words = sc .parallelize (List ("A","B","C,"D"),2 )//构建原始 RDD
words .foreach (x => println ("字母："+ x)//打印输出每个单 词构造的一句话


//lookup一一查找元素
//在 Key/ Value 型的 RDD 中，查找与参数 key 相同 Key 值的元素，并得到这些元素 的 Value 值构成的序列 。

val pairs = sc.parallelize(List("apple","banana","berry","cherry","curnquat","haw") ,1).keyBy(_.length) //构建原始 RDD
pairs.collect
pairs.lookup(5) //查找长度key为 5 的单词


//max一一求最大值
返回 RDD 中值最大的元素，可以通过参数 comp 指定元素间比较大小的方法 。

val nums=sc.parallelize(O to9,1) //构建由 0 -9 组成的 RDD
nums.max 1//寻找 RDD 中最大的值

min一一求最小值


//take一一获取前n个元素
- take (num: Int): List [T)
//以数组的方式返回 RDD 中的前 num 个元素。

val sample= sc.parallelize (0 to 4 ,1) //构建 0 -4 构成的 ROD
sample.take(2)   //返回ROD中的前两个元素


//• takeOrdered一一获取排序后的前 n 个元素
//-takeOrdered(num: Int): List[T]

//以数组的方式返回 RDD 中的元素经过排序后的前 num 个元素 。

val sample= sc.parallelize(List (2 ,1,4 ,3 ,0) ,1)  //构建 0 - 4 乱序后 构成的 ROD
sample.takeOrdered(2)//返回 ROD排序后的前 2 个元素



//takeSample一一提取 n 个样本
//- takeSample (withReplacement: Boolean, num: Int , seed: Long): List [T]

//随机提取 RDD 中一定数量的样本元素，并以数组方式返回 。 参数 withReplacement 指定是否为放回取样，
参数 num 指定提取的样本数量，参数 seed 为随 机种子。

val sample=sc.parallelize(0 to 9,1)  //构建0-9构成的ROD sample
sample.takeSample(true ,3 ,5) //以有放回取样的方式选取 3 个样本元素

sample.takeSample (false, 3 , 5) // 以无放回取样的方式选取 3 个样本元素


//top一一寻找值最大的前几个元素
//返回 RDD 中值最大的前 num 个元素，可以通过参数 comp 指定元素间比较大小的 方法。

val nums=sc.parallelize(0 to 9,1)//构建由0-9组成的ROD
nums.top(3) //寻找ROD中最大的三个元素



//存储型行动算子
//saveAsObjectFile一一存储为二进制文件
- stweAsObjectFile (path : String）: Unit
//将 RDD 转换为序列号对象后，以 Hadoop SequenceFile 文件格式保存，保存路径由 参数 path指定。

val data= sc.parallelize (0 to 9 ,1) //构建 0 -9 组成的 ROD
data. saveAsObjectFile"obj")   //将 ROD 以 SequenceFile 文件格式保 存，文件名为 obj



//saveAsTextFile一一存储为文本文件
//将 RDD 以文本文件格式保存，保存路径由参数 path 指定 。 如果要节省存储空间， 还可以选择使用该接口的另一种方式
saveAsTextFile(path: String， codec: Class [ _ < : CompressionCodec J)，可以使用参数 codec 指定压缩方式 。


val data= sc .parallelize (0 to 9 ,l) II构建 0 -9 组成的 RDD
data.saveAsObjectFile ("text")  //将 RDD 以文本文件格式保存，文件名 为 text



//• saveAsNewAPIHadoopFile一一存储为 Hadoop 文件

val pairs = sc.parallelize (List ("apple","banana",”berry","cherry","cumquat", "haw") ,1).keyBy(_.length) //构建原始 RDD
pairs.saveAsNewAPIHadoopFile[TextOutputFormat[LongWritable, String]] (hFilel)



//saveAsNewAPilladoopDataset-一存储为 Hadoop 数据集
- saveAsNewAP工HadoopDataset(co口f : Configuration): U口it

将·Key/ Value 型 RDD 保存为 Hadoop 数据集，该接口一般用于将 RDD 存储到如 HBase 之类的数据库中 。

Configuration conf =HBaseConfiguration .create () //构造 HBase 配置
conf.set(TableinputFormat.INPUT_TABLE，”user”)//设置使用的 HBase表名
pairRDD.saveAsNewAP工HadoopDataset (conf) //将 RDD 数据写入 HBase 中




================
//缓存算子
cache一一缓存 ROD
//cache 将 RDD 的数据持久化存储在内存中，其实现方法是使用后面我们会介绍的

val nurn=sc.parallelize(O to9,l) //构建 RDD
nurn.getStorageLevel //显示 RDD 当前的持久化状态
nurn.persist()    //使用 persist 进行默认的 MEMORY_ONLY 持久化
nurn.getStorageLevel //显示 RDD 新 的持久化状态







*/










  }
}
