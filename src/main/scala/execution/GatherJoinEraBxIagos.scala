package execution

import bachelorthesis.{GatherJoinRDD, LoadNetCdfRDD, ReadMode}
import bachelorthesis.utilsExternal.{FileName, Position, Var, eraBGatherJoin, iagosLoadConfig, localDataRoot}
import org.apache.spark.{SparkConf, SparkContext}

object GatherJoinEraBxIagos {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TestApplication")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    // Define IAGOS Files that you want to join
    val iagosFiles: Seq[FileName] = Seq(localDataRoot + "IAGOS/IAGOS_timeseries_2019081509303904.nc")

    // Define IAGOS Variables you want to compare
    // (lat and lon are needed for GatherJoin and need to be explicitly mentioned since they are no dimension variables)
    val iagosVariables: Seq[FileName] = Seq("lat", "lon", "air_speed_AC")

    // Define Meta Information about your ERA Files
    val eraBasePath = localDataRoot + "ERA"
    val eraFilePrefix = "B"

    // Define ERA Variables you want to compare
    val eraVariables = Seq("u10")

    // Read the IAGOS Files
    val iagosRead = new LoadNetCdfRDD(sc, iagosFiles, iagosVariables, iagosLoadConfig(true))

    // Map their values to the ERA FileNames and Positions
    // (Implementation found in ’bachelorthesis’)
    val iagosMap = iagosRead
      .map { case (file: FileName, _: Position, vars: Seq[Var], vals: Seq[Double]) => {
        val timeDouble = vals.head
        val latitude = vals(1)
        val longitude = vals(2)
        eraBGatherJoin(eraBasePath, eraFilePrefix, timeDouble, latitude, longitude, eraVariables, file, vars, vals)
      }}
    // Carry out the Join with the ReadMode of your choice
    val join = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosMap, ReadMode.cuboid("manhattan", 350))

    //join.foreach(println)
    println(join.count())
  }
}