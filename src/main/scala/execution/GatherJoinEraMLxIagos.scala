package execution

import bachelorthesis.{GatherJoinRDD, LoadNetCdfRDD, ReadMode}
import bachelorthesis.utilsExternal.{FileName, Position, Var, eraBGatherJoin, eraMLGatherJoin, iagosLoadConfig, localDataRoot}
import org.apache.spark.{SparkConf, SparkContext}

object GatherJoinEraMLxIagos {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TestApplication")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    // Define IAGOS Files that you want to join
    val iagosFiles: Seq[FileName] = Seq(localDataRoot + "IAGOS/IAGOS_timeseries_2019081509303904.nc")

    // Load IAGOS Air Pressure to later compute ERA ML Model Level
    // (lat an lon are needed for GatherJoin and need to be explicitly mentioned since they are no dimension variables)
    val iagosVariables: Seq[FileName] = Seq("lat", "lon", "air_press_AC")

    // Define Meta Information about your ERA Files
    val eraBasePath = localDataRoot + "ERA"
    val bPrefix = "B"
    val mlPrefix = "ml"

    // Load ERA B Surface Pressure to later compute ERA ML Model Level
    val bVariables = Seq("sp")

    // Define ERA ML Variables you want to compare
    val eraMLVariables = Seq("cc")

    // Read the IAGOS Files
    val iagosRead = new LoadNetCdfRDD(sc, iagosFiles, iagosVariables, iagosLoadConfig(true))

    // Map their values to the ERA FileNames and Positions
    // (Implementation found in ’bachelorthesis’)
    val iagosMapB = iagosRead
      .map { case (file: FileName, _: Position, vars: Seq[Var], vals: Seq[Double]) => {
        val timeDouble = vals.head
        val latitude = vals(1)
        val longitude = vals(2)
        eraBGatherJoin(eraBasePath, bPrefix, timeDouble, latitude, longitude, bVariables, file, vars, vals)
      }}
    // Carry out the Join with the ReadMode of your choice
    val joinB = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosMapB, ReadMode.noDuplicates)
    // Prepare for ML Join
    // (Just a Simplification of the Elements)
    val combine = joinB.map {case (_: FileName, _: Position, (bVars: Seq[Var], bVals: Seq[Double]), (iagosFile: FileName, iagosVars: Seq[Var], iagosVals: Seq[Double])) => {
      (iagosFile, iagosVars ++ bVars, iagosVals ++ bVals)
    }}
    // Map their values to the ERA FileNames and Positions
    // (Implementation found in ’bachelorthesis’)
    val iagosMapML = combine
      .map { case (file: FileName, vars: Seq[Var], vals: Seq[Double]) => {
        val timeDouble = vals.head
        val latitude = vals(1)
        val longitude = vals(2)
        val airPressure = vals(3)
        val surfacePressure = vals(4)
        eraMLGatherJoin(eraBasePath, mlPrefix, timeDouble, latitude, longitude, airPressure, surfacePressure, eraMLVariables, file, vars, vals)
      }}
    // Carry out the Join with the ReadMode of your choice
    val joinML = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosMapML, ReadMode.noDuplicates)
    joinML.foreach(println)
  }
}