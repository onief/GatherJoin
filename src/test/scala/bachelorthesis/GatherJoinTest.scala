package bachelorthesis

import bachelorthesis.utilsExternal.{FileName, Position, Var, eraBGatherJoin, eraMLGatherJoin, iagosLoadConfig, localDataRoot}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class GatherJoinTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  private var correctJoinData: Array[(FileName, Seq[Var], Seq[Double], FileName, Seq[Var], Seq[Double])] = _
  private var correctJoinMLData: Array[(FileName, Seq[Var], Seq[Double], FileName, Seq[Var], Seq[Double])] = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("GatherJoinTest")
      .master("local[*]")
      .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val source = Source.fromFile("Yields/TestData/normalJoin.csv")
    correctJoinData = source.getLines().map(_.split(",")).map(arr => (arr.head, arr(1).split(":").toSeq, arr(2).split(":").toSeq.map(_.toDouble),
      arr(3), arr(4).split(":").toSeq, arr(5).split(":").toSeq.map(_.toDouble))).toArray
    source.close()

//    val sourceML = Source.fromFile("Yields/TestData/normalMLJoin.csv")
//    correctJoinMLData = sourceML.getLines().map(_.split(",")).map(arr => (arr.head, arr(1).split(":").toSeq, arr(2).split(":").toSeq.map(_.toDouble),
//      arr(3), arr(4).split(":").toSeq, arr(5).split(":").toSeq.map(_.toDouble))).toArray
//    sourceML.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  private def testGatherJoin(readMode: ReadMode): Unit = {
    val iagosFilePath = localDataRoot + "IAGOS/IAGOS_timeseries_2019081509303904.nc"
    val eraBasePath = localDataRoot + "ERA"
    val eraFilePrefix = "B"

    val iagosReadGatherJoin = new LoadNetCdfRDD(sc, Seq(iagosFilePath), Seq("lat", "lon", "air_speed_AC"), iagosLoadConfig(true))

    val gatherJoin = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosReadGatherJoin.map { case (baseFile: FileName, _: Position, baseVars: Seq[Var], baseVals: Seq[Double]) => {
      eraBGatherJoin(eraBasePath, eraFilePrefix, baseVals.head, baseVals(1), baseVals(2), Seq("u10"), baseFile, baseVars, baseVals)
    }}, readMode)

    val testData = gatherJoin
      .collect()
      .sortBy { case (eraFile: FileName, pos: Position, (joinedVars: Seq[Var], joinedVals: Seq[Double]), (baseFile: FileName, baseVars: Seq[Var], baseVals: Seq[Double])) => {
        (baseVals.head, baseVals(1), baseVals(2))
      }}
      .map { case (eraFile: FileName, pos: Position, (joinedVars: Seq[Var], joinedVals: Seq[Double]), (baseFile: FileName, baseVars: Seq[Var], baseVals: Seq[Double])) => {
        (eraFile, joinedVars, joinedVals, baseFile, baseVars, baseVals)
      }}

    assert(testData.length == correctJoinData.length)

    testData.indices.foreach(i => {
      assert(testData(i)._1 == correctJoinData(i)._1)
      assert(testData(i)._2.last == correctJoinData(i)._2.last)
      assert(testData(i)._3.last == correctJoinData(i)._3.last)
      assert(testData(i)._4 == correctJoinData(i)._4)
      assert(testData(i)._5 == correctJoinData(i)._5)
      assert(testData(i)._6 == correctJoinData(i)._6)
    })
  }

  private def testGatherJoinML(readMode: ReadMode): Unit = {
    val iagosFilePath = localDataRoot + "IAGOS/IAGOS_timeseries_2019081509303904.nc"
    val eraBasePath = localDataRoot + "ERA"
    val bFilePrefix = "B"
    val mlFilePrefix = "ml"

    val iagosRead = new LoadNetCdfRDD(sc, Seq(iagosFilePath), Seq("lat", "lon", "air_press_AC", "CO_P1"), iagosLoadConfig(true))

    val iagosMapB = iagosRead
      .map { case (file: FileName, _: Position, vars: Seq[Var], vals: Seq[Double]) => {
        val timeDouble = vals.head
        val latitude = vals(1)
        val longitude = vals(2)
        eraBGatherJoin(eraBasePath, bFilePrefix, timeDouble, latitude, longitude, Seq("sp"), file, vars, vals)
      }}
    val joinB = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosMapB, readMode)
    val combine = joinB.map {case (_: FileName, _: Position, (bVars: Seq[Var], bVals: Seq[Double]), (iagosFile: FileName, iagosVars: Seq[Var], iagosVals: Seq[Double])) => {
      (iagosFile, iagosVars ++ bVars, iagosVals ++ bVals)
    }}
    val iagosMapML = combine
      .map { case (file: FileName, vars: Seq[Var], vals: Seq[Double]) => {
        val timeDouble = vals.head
        val latitude = vals(1)
        val longitude = vals(2)
        val airPressure = vals(3)
        val surfacePressure = vals(5)
        eraMLGatherJoin(eraBasePath, mlFilePrefix, timeDouble, latitude, longitude, airPressure, surfacePressure, Seq("cc"), file, vars, vals)
      }}
    val joinML = new GatherJoinRDD[(FileName, Seq[Var], Seq[Double])](iagosMapML, ReadMode.noDuplicates)

    val testData = joinML
      .collect()
      .sortBy { case (mlFile: FileName, pos: Position, (mlVars: Seq[Var], mlVals: Seq[Double]), (iagosFile: FileName, joinVars: Seq[Var], joinVals: Seq[Double])) => {
        (joinVals.head, joinVals(1), joinVals(2), joinVals(3))
      }}
      .map { case (mlFile: FileName, pos: Position, (mlVars: Seq[Var], mlVals: Seq[Double]), (iagosFile: FileName, joinVars: Seq[Var], joinVals: Seq[Double])) => {
        (mlFile, mlVars, mlVals, iagosFile, joinVars, joinVals)
      }}

    assert(testData.length == correctJoinMLData.length)

    testData.indices.foreach(i => {
      assert(testData(i)._1 == correctJoinMLData(i)._1)
      assert(testData(i)._2.last == correctJoinMLData(i)._2.last)
      assert(testData(i)._3.last == correctJoinMLData(i)._3.last)
      assert(testData(i)._4 == correctJoinMLData(i)._4)
      assert(testData(i)._5 == correctJoinMLData(i)._5)
      assert(testData(i)._6 == correctJoinMLData(i)._6)
    })
  }

  test("Naive Gather Join yields correct results") {
    testGatherJoin(ReadMode.naive)
  }

  test("No Duplicates Gather Join yields correct results") {
    testGatherJoin(ReadMode.noDuplicates)
  }

  test("Cuboid Gather Join (Volume = 10.0) yields correct results") {
    testGatherJoin(ReadMode.cuboid("squaredEuclidean", 10.0))
  }

  test("Cuboid Gather Join (Volume = 50.0) yields correct results") {
    testGatherJoin(ReadMode.cuboid("squaredEuclidean", 50.0))
  }

  test("Cuboid Gather Join (Volume = 100.0) yields correct results") {
    testGatherJoin(ReadMode.cuboid("squaredEuclidean", 100.0))
  }

  test("Cuboid Gather Join (Volume = 500.0) yields correct results") {
    testGatherJoin(ReadMode.cuboid("squaredEuclidean", 500.0))
  }
}