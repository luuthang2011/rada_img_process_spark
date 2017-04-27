package pack




import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting


/**
  * Created by magicghost_vu on 09/04/2017.
  */
object Main {


    def calSubArrayFrom2DArray(windowSize:Int, rowIndex:Int, columnIndex:Int, arr:Array[Array[String]]): Array[Int] ={
        val numCellAround:Int= windowSize/2
        //1. find i, j to start
        //2. find i, j to end
        //3. get array

        var rowIndexStart= rowIndex
        var columnIndexStart= columnIndex
        //var rowIndexEnd= rowIndexEnd
        var i=0
        while (i<numCellAround && rowIndexStart>0){
            rowIndexStart-=1
            i+=1
        }
        i=0
        while (i<numCellAround&& columnIndexStart>0){
            columnIndexStart-=1
            i+=1
        }
        i=0
        var rowIndexEnd= rowIndex
        var columnIndexEnd= columnIndex
        while (i<numCellAround&& columnIndexEnd<arr(0).length-1){
            columnIndexEnd+=1
            i+=1
        }
        i=0
        while(i<numCellAround && rowIndexEnd<arr.length -1){
            rowIndexEnd+=1
            i+=1
        }

        val arrBuffer:ArrayBuffer[Int]= new ArrayBuffer[Int]()
        for(j <- rowIndexStart to rowIndexEnd){
            for(k <- columnIndexStart to columnIndexEnd){
                arrBuffer.append(arr(j)(k).toInt)
            }
        }
        arrBuffer.toArray
    }

    def calStandardDeviation(arr: Array[Int]):Double={
        val average= arr.reduce(_+_)/(arr.length+0.0)
        val devs= arr.map(element=>{
            (element-average)*(element-average)+0.0
        })
        Math.sqrt(devs.reduce(_+_)/(arr.length-1.0))
    }

    def calAverage(arr:Array[Int]):Double={
        arr.reduce(_+_)/(arr.length+0.0)
    }

    def calHistogram(allNumber:RDD[String]):RDD[(String, Int)]={
        allNumber.map(number=> (number, 1)).reduceByKey(_+_)
    }

    def main(args: Array[String]): Unit = {

        //can process a img 1024*1024, how about multiple ??
        // need rebroadcast


        val threshold_1:Double=0.9
        val threshold_2:Double=0.99

        var f1:Int=0
        var f2:Int=0

        val sizeWin:Int= 3

        val configSp:SparkConf= new SparkConf().setAppName("Simple App")
        val sparkContext:SparkContext= new SparkContext(configSp)
        val textFile:RDD[String]= sparkContext.textFile("/data/IMAGERY.TIF_0_4096.csv")
        val allLineRdd:RDD[String]= textFile.filter(line=>{
            if(line.length==1||line.length==0|| line.length<2000) false
            else true
        })
        val allNumberRdd:RDD[String]= allLineRdd.flatMap(line=>line.split(","))
        allNumberRdd.persist()

        /*val arrayAllNumSize:Array[Int]= Array[Int](allNumberRdd.count().toInt)
        sparkContext.parallelize(arrayAllNumSize).saveAsTextFile("/allNumberSize")*/

        val histogramRdd:RDD[(String, Int)]= calHistogram(allNumberRdd)
        //histogramRdd.saveAsTextFile("/histogram")






        //histogramRdd.saveAsTextFile("/histogram1")

        //those variale had return for driver program, collect() called
        // those variable cannot be used in rdd operation, because it is not available for all executors
        // so you should broadcast it to all executors
        val allNumberArr:Array[String]= allNumberRdd.collect()
        //at here, histogram had been sorted
        val histogramArr:Array[(String, Int)]= histogramRdd.collect()

        //sort
        Sorting.stableSort(histogramArr, (h1:(String, Int), h2:(String, Int))=>{
            h1._1.toInt>h2._1.toInt
        })

        sparkContext.parallelize(histogramArr).saveAsTextFile("/histogramArrSorted")

        val mapHistogram:mutable.HashMap[String, Int]= new mutable.HashMap[String, Int]()
        histogramArr.foreach(t=>{
            mapHistogram.put(t._1, t._2)
        })

        val arrKeysHistogram:Array[String]= histogramArr.map(t=>t._1)

        var m=0
        var totalPixel_1=0
        // m at 0.90
        while (m < arrKeysHistogram.length-1 && totalPixel_1 <= threshold_1* 1024 * 1024){
            totalPixel_1+= mapHistogram(arrKeysHistogram(m))
            m+=1
        }
        var totalPixel_2=0
        var n=0
        // n at 0.99
        while (n < arrKeysHistogram.length-1 && totalPixel_2 <= threshold_2* 1024 * 1024){
            totalPixel_2+= mapHistogram(arrKeysHistogram(n))
            n+=1
        }


        val valueAt90= arrKeysHistogram(m)
        val valueAt99= arrKeysHistogram(n)

        val arrThreshold:Array[String]= Array[String](n.toString,  m.toString, valueAt90, valueAt99)
        val rddThreshold= sparkContext.parallelize(arrThreshold)
        rddThreshold.saveAsTextFile("/rddThresold")

        sparkContext.parallelize(arrKeysHistogram).saveAsTextFile("/rddArrKeysHistogram")


        val cm= m+1.0
        val ce= n+1.0


        val cd= cm/ce





        //calculate intensity
        val intensityArray:Array[Double]= allNumberArr.map(value=>{
            if(mapHistogram.get(value).isEmpty){
                0.0
            }else{
                mapHistogram(value) / (1024 * 1024.0)
            }
        })
        val iteratorArr : Iterator[Array[String]]= allNumberArr.grouped(1024)
        val tmpArr2D:Array[Array[String]]= iteratorArr.toArray
        val arr2D= tmpArr2D.filter(arr=>{
            if(arr.length!=1024) false
            else true
        })
        val arrayBufferFilter:ArrayBuffer[Double]= new ArrayBuffer[Double]()
        val arrayIndexOutOfBoundsException:ArrayBuffer[(Int, Int)]= new ArrayBuffer[(Int, Int)]()
        val arrayNumFormatException:ArrayBuffer[(Int, Int)]= new ArrayBuffer[(Int, Int)]()


        //calculate filter
        for(i <-0 to arr2D.length-1){
            for(j <- 0 to arr2D(0).length-1){
                try {
                    val tmpNum:Double= arr2D(i)(j).toDouble
                    val surroundCells= calSubArrayFrom2DArray(sizeWin, i, j, arr2D)
                    val standardDeviation = calStandardDeviation(surroundCells)
                    if(standardDeviation.equals(0.0)){
                        arrayBufferFilter.append(0)
                    }else{
                        arrayBufferFilter.append(tmpNum/standardDeviation)
                    }
                }catch{
                    case a:NumberFormatException => {
                        arrayNumFormatException.append((i, j))
                        arrayBufferFilter.append(0.0)
                    }
                    case b: IndexOutOfBoundsException => {
                        arrayBufferFilter.append(0.0)
                        arrayIndexOutOfBoundsException.append((i, j))
                    }
                }

            }
        }






        val filterArr:Array[Double]= arrayBufferFilter.toArray
        val rddFilter:RDD[Double]= sparkContext.parallelize(filterArr)
        rddFilter.saveAsTextFile("/rddFilter")
        val rddErr:RDD[(Int, Int)]= sparkContext.parallelize(arrayIndexOutOfBoundsException)
        rddErr.saveAsTextFile("/err")
        //sparkContext.parallelize(arrayIndexOutOfBoundsException).saveAsTextFile("/outBoundEx")
        //sparkContext.parallelize(arrayNumFormatException).saveAsTextFile("/numformatEx")
        sparkContext.parallelize(intensityArray).saveAsTextFile("/intensity")



        val arrBufferMerge:ArrayBuffer[(Double, Double)]=new ArrayBuffer[(Double, Double)]()


        for(i <- 0 to filterArr.length-1){
            arrBufferMerge.append((intensityArray(i),filterArr(i)))
        }


        val rddMerge:RDD[(Double, Double)]= sparkContext.parallelize(arrBufferMerge)



        val resultRdd:RDD[Double]= rddMerge.map(e=>{
            e._1*cd+(1-cd)*e._2
        })

        resultRdd.saveAsTextFile("/rddResult")
        allNumberRdd.unpersist()

    }
}
