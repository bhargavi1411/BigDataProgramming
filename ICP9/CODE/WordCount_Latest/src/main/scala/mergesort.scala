package spark.code

import org.apache.log4j.{Level, Logger}
import org.apache.spark._


object MergeSort {

  def main(args: Array[String]): Unit = {


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\winutils" )

    //Spark Context

    val conf =   new SparkConf().setAppName("mergeSort").setMaster("local");
    val sc   =   new SparkContext(conf);

    val a = Array(9, 5, 16, 3, 4, 11, 8, 12);

    val b = sc.parallelize(Array(9, 5, 16, 3, 4, 11, 8, 12));


    val maparray = b.map(x=>(x,1))

    val sorted = maparray.sortByKey();


    //Printing the RDD before Sort
    sorted.keys.collect().foreach(println)


    val n = a.length;

    val l = 0;

    val r = n-1;

    print("\nArray Before Sort\n")
    for ( x <- a)
    {
      print(x+",");
    }

    sort(a,l,r);

    print("\nArray after Sort\n")
    for ( y <- a)
    {
      print(y+",")
    }



    // Sorting Array
    def sort(arr: Array[Int], l: Int, r: Int): Unit = {
      if (l < r) { // Find the middle point
        val m = (l + r) / 2
        // Sort first and second halves
        sort(arr, l, m)
        sort(arr, m + 1, r)
        // Merge the sorted halves
        merge(arr, l, m, r)
      }
    }


    // Merge for Array
    def merge(arr: Array[Int], l: Int, m: Int, r: Int): Unit = {

      // Find sizes of two subarrays to be merged
      val n1 = m - l + 1
      val n2 = r - m


      /* Create temp arrays */
      val L = new Array[Int](n1)
      val R = new Array[Int](n2)

      /*Copy data to temp arrays*/

      var a = 0
      while (a < n1){
        L(a) = arr(l + a);
        a += 1;
      }
      var b = 0
      while (b < n2){
        R(b) = arr(m + 1 + b);
        b += 1;
      }


      /* Merge the temp arrays */
      // Initial indexes of first and second subarrays

      var i = 0
      var j = 0
      // Initial index of merged subarry array
      var k = l
      while (i < n1 && j < n2) {
        if (L(i) <= R(j)) {
          arr(k) = L(i)
          i += 1
        }
        else {
          arr(k) = R(j)
          j += 1
        }
        k += 1
      }

      /* Copy remaining elements of L[] if any */
      while (i < n1)
      {
        arr(k) = L(i)
        i += 1
        k += 1
      }

      /* Copy remaining elements of R[] if any */
      while (j < n2)
      {
        arr(k) = R(j)
        j += 1
        k += 1
      }

    }

  }



}

/Users/bhargavichennupati/Downloads/WordCount_Latest/src/main/scala/mergesort.scala