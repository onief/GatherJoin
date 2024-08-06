package bachelorthesis

import bachelorthesis.utilsExternal.{arrayEquals, eraLoadConfig, localDataRoot}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class LoadNetCdfTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("GatherJoinTest")
      .master("local[*]")
      .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("NetCdf Files are read correctly") {
    val positions = Seq(Array(0, 36, 454),
      Array(0, 79, 766),
      Array(0, 96, 1048),
      Array(0, 174, 1),
      Array(0, 218, 192),
      Array(0, 220, 521),
      Array(0, 266, 14),
      Array(0, 267, 1153),
      Array(0, 274, 774),
      Array(0, 281, 1094),
      Array(0, 290, 661),
      Array(0, 402, 1229),
      Array(0, 407, 1264),
      Array(0, 455, 1419),
      Array(0, 476, 683),
      Array(0, 504, 847),
      Array(0, 514, 973),
      Array(0, 591, 1251),
      Array(0, 641, 455),
      Array(0, 706, 1359))

    val realElems = Seq((Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 81.0, -66.5, 237.93)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 70.25, 11.5, 269.93)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 66.0, 82.0, 226.31)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 46.5, -179.75, 267.67)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 35.5, -132.0, 278.88)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 35.0, -49.75, 286.31)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 23.5, -176.5, 291.81)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 23.25, 108.25, 284.86)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 21.5, 13.5, 268.17)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 19.75, 93.5, 292.64)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, 17.5, -14.75, 273.20)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -10.5, 127.25, 297.25)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -11.75, 136.0, 298.56)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -23.75, 174.75, 290.42)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -29.0, -9.25, 289.85)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -36.0, 31.75, 292.56)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -38.5, 63.25, 280.36)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -57.75, 132.75, 277.31)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -70.25, -66.25, 260.81)),
      (Seq("time", "latitude", "longitude", "d2m"), Seq(1.4832288E9, -86.5, 159.75, 243.15)))

    val loadedElems = new LoadNetCdfRDD(sc, Seq(localDataRoot + "ERA_B_shifted/B_shifted_20170101_00.nc"), Seq("d2m"), eraLoadConfig(1))
      .filter(e => {
        positions.map(pos => arrayEquals(pos, e._2)).reduce(_ || _)
      })
      .collect()
      .sortBy(e => (e._2.head, e._2(1), e._2(2)))
      .map(e => (e._3, e._4))

    assert(loadedElems.length == realElems.length)

    loadedElems.indices.foreach(i => {
      val (lVars, lVals) = loadedElems(i)
      val (rVars, rVals) = realElems(i)
      assert(lVars == rVars)
      assert(lVals.init == rVals.init)
      assert(math.abs(lVals.last - rVals.last) < 0.01)
    })

  }
}