package com.github.graph
/**
 * Created by hadoop on 4/9/16.
 */
object Analytics {
  /*def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions> [other options]")
      System.err.println("Supported 'taskType' as follows:")
      System.err.println("  pagerank    Compute PageRank")
      System.err.println("  cc          Compute the connected components of vertices")
      System.err.println("  triangles   Count the number of triangles")
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    //drop: drop(n: Int): List[A] 丢弃前n个元素，返回剩下的元素
    val optionsList = args.drop(2).map { arg =>
      //dropWhile(p: (A) ⇒ Boolean): List[A] 从左向右丢弃元素，直到条件p不成立
      //val nums = List(1,1,1,1,4,4,4,4)
      //val left = nums.drop(4)   // List(4,4,4,4)
      //val right = nums.dropRight(4) // List(1,1,1,1)
      //  val tailNums = nums.dropWhile( _ == nums.head)  // List(4,4,4,4)

      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()

    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }

    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_)) //fromString(String name) 方法被用作toString()方法中所述创建的字符串标准来表示一个UUID
    val edgeStorageLevel = options.remove("edgeStorageLevel")
        .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRank(" + fname + ")"))

        ////加载边时顶点是边上出现的点
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        //fold函数将一种格式的输入数据转化成另外一种格式返回http://www.iteblog.com/archives/1228
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = (numIterOpt match {
          //在没有值的时候，使用None，这是Option的一个子类。如果有值可以引用，就使用Some来包含这个值。Some也是Option的子类
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()

      case "cc" =>
        options.foreach {
          //IllegalArgumentException此异常表明向方法传递了一个不合法或不正确的参数。你看看传值的方法是否参数不正确
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("ConnectedComponents(" + fname + ")"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()

        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        //GraphX提供了ConnectedComponents和StronglyConnected-Components算法，使用它们可以快速计算出相应的连通图
        val cc = ConnectedComponents.run(graph)
        println("Components: " + cc.vertices.map { case (vid, data) => data }.distinct())
        sc.stop()

      case "triangles" =>
        options.foreach {

          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("TriangleCount(" + fname + ")"))

        //graphLoader是graphx中专门用于图的加载和生成，最重要的函数就是edgeListFile，定义如下
        //http://www.csdn.net/article/2014-06-18/2820283
        val graph = GraphLoader.edgeListFile(sc, fname,
          canonicalOrientation = true,

          //numEdgePartitions 是分区数
          numEdgePartitions = numEPart,
          //Graph的存储是分EdgeRDD和VertexRDD两块，可以分别设置StorageLevel。默认是内存。
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel)
          // TriangleCount requires the graph to be partitioned
          .partitionBy(partitionStrategy.getOrElse(RandomVertexCut)).cache()
        //使用graph.triangleCount即可在Spark GraphX上进行三角形关系计算：
        //graph.TriangleCount.vertices
        val triangles = TriangleCount.run(graph)
        println("Triangles: " + triangles.vertices.map {
          case (vid, data) => data.toLong
        }.reduce(_ + _) / 3)
        sc.stop()

      case _ =>
        println("Invalid task type.")
    }
  }*/
}
