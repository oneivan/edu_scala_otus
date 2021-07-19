import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI
import scala.io.Source

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io._

object Main extends App {
  val conf = new Configuration()
  val hdfs_fs = FileSystem.get( new URI("hdfs://localhost:9000"), conf )
  val hdfs_f  = new Path("/stage/date=2020-12-01/part-0001.csv.inprogress")

  // create dir for output:
  val out_pth = new Path( "/ods")
  if ( ! hdfs_fs.exists( out_pth ) ) {
    hdfs_fs.mkdirs( out_pth )
  }

  val in = hdfs_fs.open( hdfs_f )
  println( in.readLine() )
  in.close()
}
