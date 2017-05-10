package test

/*import pack.Main*/

import scala.util.Sorting

/**
  * Created by magicghost_vu on 26/04/2017.
  */
object Test {
    def main(args: Array[String]): Unit = {
        /*val array:Array[Array[String]]= new Array[Array[String]](10)
        var count:Int=0
        for(i<-0 to 9){
            array(i)= new Array[String](10)
            for(j<-0 to 9){
                array(i)(j)= count.toString
                count+=1
            }
        }
        //print(array)

        val arrayRes= Main.calSubArrayFrom2DArray(3, 9 ,0  , array)
        println(arrayRes)


        print(0.0.equals(0.0))*/



        val arrTuple2:Array[(String, Int)]= Array[(String, Int)](("1", 2), ("4", 5),("3", 6))
        arrTuple2.sortWith((a,b)=>{
            a._1.toInt > b._1.toInt
        })

        Sorting.stableSort(arrTuple2, (t1:(String, Int), t2:(String, Int))=>{
            t1._1.toInt>t2._1.toInt
        })
        print()
    }
}
