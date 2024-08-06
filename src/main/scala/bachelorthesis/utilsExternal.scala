package bachelorthesis

import bachelorthesis.utilsInternal.{binarySearch, computePressureFromSP, doubleToInstant, instantToHourFilePath, latToIndex, longToIndex}

import java.io.File
import java.nio.file.Paths

object utilsExternal {

  type Position = Array[Int]
  type FileName = String
  type Var = String

  val projectRoot: String = Paths.get(".").toAbsolutePath.toString.init
  //TODO: Add data root
  val localDataRoot: String = ""

  def iagosLoadConfig(roundToClosestHour: Boolean): LoadConfig = {
    LoadConfig(
      maxCoordsPerPartition = 43260,
      nullPositions = false,
      readDimensions = true,
      roundTimeToClosestHour = roundToClosestHour,
      sharedStructure = false
    )
  }

  def eraLoadConfig(numberOfVariables: Int): LoadConfig = {
    LoadConfig(
      maxCoordsPerPartition = (43260 * math.pow(0.93  , (numberOfVariables - 1).toDouble)).toInt,
      nullPositions = false,
      readDimensions = true,
      roundTimeToClosestHour = true,
      sharedStructure = true
    )
  }

  def arrayEquals(arr1: Array[Int], arr2: Array[Int]): Boolean = {
    val arrLen = arr1.length
    if (arrLen != arr2.length) false
    else {
      (0 until arrLen).map(i => arr1(i) == arr2(i)).reduce((x, y) => x && y)
    }
  }

  def timeDoubleToHourFilePath(time: Double, basePath: String, ncFilePrefix: String, fileExtension: String = ".nc"): FileName = {
    instantToHourFilePath(doubleToInstant(time), basePath, ncFilePrefix, fileExtension)
  }

  def roundTo25Grid(d: Double): Double = {
    Math.round(d / 0.25) * 0.25
  }

  // expects latitude in [-90, 90] and longitude in [-180, 179.75]
  def eraBGatherJoin(eraBasePath: String, eraFilePrefix: String, timeDouble: Double, latitude: Double, longitude: Double, joinVariables: Seq[Var],
                     baseFile: FileName, baseVariables: Seq[Var], baseValues: Seq[Double]):
  (FileName, Position, Seq[Var], (FileName, Seq[Var], Seq[Double])) = {

    (timeDoubleToHourFilePath(timeDouble, eraBasePath, eraFilePrefix), Array(0, latToIndex(latitude), longToIndex(longitude)), joinVariables, (baseFile, baseVariables, baseValues))
  }

  // Is used to clean up RDD after B join to prepare vor ML Join
  def combineJoin(baseFileName: FileName, baseVariables: Seq[Var], baseValues: Seq[Double],
                  joinedVariables: Seq[Var], joinedValues: Seq[Double]): (FileName, Seq[Var], Seq[Double]) = {
    (baseFileName, baseVariables ++ joinedVariables, baseValues ++ joinedValues)
  }

  // Compute ERA ML Model Level from pressure and surface pressure
  def computeModelLevel(pressure: Double, surfacePressure: Double): Int = {
    val modelLevelPressures = new IndexedSeq[Double] {
      override def length: Int = 137
      override def apply(idx: Int): Double = computePressureFromSP(idx, surfacePressure)
    }
    binarySearch(modelLevelPressures, pressure) + 1
  }

  def computeEraMLHeightIdx(pressure: Double, surfacePressure: Double): Int = {
    computeModelLevel(pressure, surfacePressure) - 40
  }

  // TODO: Export Model Level to Result Variables
  def eraMLGatherJoin(eraBasePath: String, eraFilePrefix: String, timeDouble: Double, latitude: Double, longitude: Double, pressure: Double, surfacePressure: Double,
                      joinVariables: Seq[Var], baseFile: FileName, baseVariables: Seq[Var], baseValues: Seq[Double]):
  (FileName, Position, Seq[Var], (FileName, Seq[Var], Seq[Double])) = {
    (timeDoubleToHourFilePath(timeDouble, eraBasePath, eraFilePrefix), Array(0, computeEraMLHeightIdx(pressure, surfacePressure), latToIndex(latitude), longToIndex(longitude)), joinVariables, (baseFile, baseVariables, baseValues))
  }

  def getListOfFiles(dir: FileName): Seq[FileName] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .map(_.getPath).toSeq
  }
}
