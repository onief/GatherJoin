package bachelorthesis

import bachelorthesis.utilsExternal.{FileName, Position, Var}
import bachelorthesis.utilsInternal._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable


/** Enum of the different Read Options of the GatherJoinRDD */
sealed trait ReadMode
object ReadMode {
  final case object naive extends ReadMode
  final case object noDuplicates extends ReadMode
  // 'squaredEuclidean', ’manhattan’ or ’volume’
  final case class cuboid(distanceMeasure: String, maxDistance: Double) extends ReadMode
}

/** Implementation of Gather Join
 *
 * @param dataToLoad RDD containing the Join Positions
 * @param readMode Defines Read Option
 */
class GatherJoinRDD[V](dataToLoad: RDD[(FileName, Position, Seq[Var], V)],
                    readMode: ReadMode)
  extends RDD[(FileName, Position, (Seq[Var], Seq[Double]), V)](dataToLoad) {

  private val hadoopConf = new Configuration(sparkContext.hadoopConfiguration) with Serializable

  private val distanceMeasures: Map[String, (Position, Position) => Int] = Map(
    "squaredEuclidean" -> squaredEuclideanDist,
    "manhattan" -> manhattanDist,
    "volume" -> manhattanDist
  )

  private val groupingMeasures: Map[String, (Position, Position) => Int] = Map(
    "squaredEuclidean" -> squaredEuclideanDist,
    "manhattan" -> manhattanDist,
    "volume" -> volumeDist
  )

  // Could be sped up by assuming FileName and/or Seq[Var] of one Partition to be identical
  override def compute(split: Partition, context: TaskContext): Iterator[(FileName, Position, (Seq[Var], Seq[Double]), V)] = {

    val toLoad = firstParent[(FileName, Position, Seq[Var], V)].iterator(split, context).toSeq

    val groupedByNcFiles: Map[FileName, Seq[(FileName, Position, Seq[Var], V)]] = toLoad
      // Group by FileName
      .groupBy { case (fileName: FileName, _: Position, _: Seq[Var], _: V) => fileName }

    // Grouping of FileName -> Different Seq[Var] that should be read from this file
    var groupedByNcVars: Map[FileName, Map[Seq[Var], Seq[(Position, V)]]] = null

    // Normal case: It only exists one Seq[Var] that should be read per File
    if (toLoad.map(_._3).distinct.length == 1) {
      groupedByNcVars = groupedByNcFiles
        .mapValues { s1: Seq[(FileName, Position, Seq[Var], V)] => Map((s1.head._3, s1.map(e => (e._2, e._4)))) }
    } else {
      groupedByNcVars = groupedByNcFiles
        .mapValues { s1: Seq[(FileName, Position, Seq[Var], V)] => {
          s1.map(e => (e._2, e._3, e._4))
            // Group by Variables
            .groupBy{ case (_: Position, variables: Seq[Var], _: V) => variables}
            .mapValues { s2: Seq[(Position, Seq[Var], V)] => s2.map(e => (e._1, e._3)) }
        }}
    }

    // Load Data from Files
    val newIter = groupedByNcVars.keys.flatMap(fileName => {
      val varMap = groupedByNcVars(fileName)

      readMode match {
        // Load Every Position
        case ReadMode.naive => naiveLoad(fileName, varMap)
        // Don't load duplicated Positions from NcVariables
        case ReadMode.noDuplicates => noDuplicatesLoad(fileName, varMap)
        // Only load Cuboid Chunks of Volume MaxSize
        case ReadMode.cuboid(distMeasure, maxSize) => cuboidLoad(fileName, varMap, distMeasure, maxSize)
      }
    }).toIterator

    new InterruptibleIterator(context, newIter)
  }

  private def naiveLoad(fileName: FileName, varMap: Map[Seq[Var], Seq[(Position, V)]]): Iterable[(FileName, Position, (Seq[Var], Seq[Double]), V)] = {
    val ncDataset = openNcDatasetInHDFS(fileName, hadoopConf)

    val output = varMap.keys.flatMap(variables => {
      val positionSeq: Seq[(Position, V)] = varMap(variables)

      positionSeq.map { case (position: Position, passThroughVals: V) => {
        val valuesPerVariableGroup: Seq[Double] = variables.map(variableName => {
          val ncVariable = ncDataset.findVariable(variableName)
          ncVariable.read(position, Array.fill(position.length) {1}).getDouble(0)
        })

        (fileName, position, (variables, valuesPerVariableGroup), passThroughVals)
      }}
    })

    fileClose(ncDataset)

    output
  }

  private def noDuplicatesLoad(fileName: FileName, varMap: Map[Seq[Var], Seq[(Position, V)]]): Iterable[(FileName, Position, (Seq[Var], Seq[Double]), V)] = {
    val ncDataset = openNcDatasetInHDFS(fileName, hadoopConf)

    val output = varMap.keys.flatMap(variables => {
      val positionSeq: Seq[(Position, V)] = varMap(variables)
      val positionGroups: Map[Seq[Int], Seq[(Position, V)]] = positionSeq.groupBy { case (pos: Position, _: V) => pos.toSeq}

      positionGroups.keys.flatMap(readPosition => {

        val distinctValuesPerVariableGroup = variables.map(variableName =>  {
          val ncVariable = ncDataset.findVariable(variableName)
          ncVariable.read(readPosition.toArray, Array.fill(readPosition.length) {1}).getDouble(0)
        })

        // Zip the values of each distinct Position to every fitting Data Point
        positionGroups(readPosition).map { case (pos: Position, passThroughVals: V) => {
          (fileName, pos, (variables, distinctValuesPerVariableGroup), passThroughVals)
        }}
      })
    })

    fileClose(ncDataset)

    output
  }

  private def cuboidLoad(fileName: FileName, varMap: Map[Seq[Var], Seq[(Position, V)]], groupMeasure: String, maxDistance: Double): Iterable[(FileName, Position, (Seq[Var], Seq[Double]), V)] = {
    val ncDataset = openNcDatasetInHDFS(fileName, hadoopConf)

    val output = varMap.keys.flatMap(variables => {
      val positionSeq: Seq[(Position, V)] = varMap(variables)
      val positionGroups: Map[Seq[Int], Seq[(Position, V)]] = positionSeq.groupBy { case (pos: Position, _: V) => pos.toSeq}
      val uniquePositions = positionGroups.keys.toSeq.map(_.toArray)

      // Sort Positions by Distance to the minimal Position
      val minPos = createMinPosition(uniquePositions)
      val sortedUniquePositions = uniquePositions.sortBy(pos => distanceMeasures(groupMeasure)(pos, minPos))
      // Create logical Groupings/Cuboids to Load
      val cuboidGrouping: mutable.Buffer[mutable.Buffer[Position]] = sortedUniquePositions.tail
        .foldLeft(mutable.Buffer(mutable.Buffer(sortedUniquePositions.head)))((agg, pos) => {
          val groupIdx = agg.length - 1
          if (groupingMeasures(groupMeasure)(agg(groupIdx).head, pos) <= maxDistance) {
            agg(groupIdx).append(pos)
          } else {
            agg.append(mutable.Buffer(pos))
          }
          agg
        })
      // Load Data from Cuboids
      cuboidGrouping.flatMap(groupPositions => {
        val min = createMinPosition(groupPositions)
        val max = createMaxPosition(groupPositions)
        val extent = max.zip(min).map { case (a: Int, b: Int) => a - b + 1}

        variables.flatMap(variableName => {
            // Slice Cuboid from Variable
            val ncVariable = ncDataset.findVariable(variableName)
            val cuboid = ncVariable.read(min, extent)
            val cuboidIndex = cuboid.getIndex

            groupPositions.map(pos => {
              // Read Positions from Cuboid
              cuboidIndex.set(pos.zip(min).map{ case (a: Int, b: Int) => a - b })
              (pos, variableName, cuboid.getDouble(cuboidIndex))
            })
          })
          .groupBy { case (position: Position, _: Var, _: Double) => position.toSeq}
          // Create correct Amount of Tuples
          .flatMap {case (position: Seq[Int], posVarVals: Seq[(Position, Var, Double)]) => {
            val variableSeq = posVarVals.map(_._2)
            val valueSeq = posVarVals.map(_._3)
            // Zip the values of each distinct Position to every fitting Data Point
            positionGroups(position).map { case (pos: Position, passThroughVals: V) => {
              (fileName, pos, (variableSeq, valueSeq), passThroughVals)
            }}
          }}
      })
    })

    fileClose(ncDataset)

    output
  }

  private def fileClose(ncDataset: NetcdfDataset): Unit = {
    if (ncDataset != null) {
      ncDataset.close()
    }
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(FileName, Position, Seq[Var], V)].partitions
  }
}
