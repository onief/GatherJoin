package bachelorthesis

import bachelorthesis.utilsExternal.{FileName, Position, Var}
import bachelorthesis.utilsInternal._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.time.Instant
import scala.collection.JavaConverters

// Scaling the Partition Size with the Number of Data Variables has to be done externally
// utilsExternal provides a convenient function
case class LoadConfig(maxCoordsPerPartition: Int,       // Defines the maximal amount of distinct Coordinates handled by each Partition
                      nullPositions: Boolean,           // Defines whether the Coordinate values of each Data Point should be exported
                      readDimensions: Boolean,          // Defines if the Dimension Variables should be read
                      roundTimeToClosestHour: Boolean,  // Defines if the Times should be casted to an hourly format rounding to the closest
                      sharedStructure: Boolean)         // Defines whether all Files of the Dataset share the same Dimension Lengths

/** Physical Data Area of Values of a NetCDF File */
case class NetCDFBlock(origin: Seq[Int], end: Seq[Int]) extends Serializable


/** Spark Partition for distributed Data Load containing a Data Area of multiple NetCDF Files */
class LoadNetCdfPartition(id: Int,
                          val fileName: FileName,
                          val dataVariables: Seq[Var],
                          val dimVariables: Seq[Var],
                          val origin: Array[Int],
                          val end: Array[Int]) extends Partition {

  override def index: Int = id
}


/** Baseline Implementation - Load NetCDF Files completely */
class LoadNetCdfRDD(sc: SparkContext,
                    fileNames: Seq[FileName],
                    dataVariables: Seq[Var],
                    loadConfig: LoadConfig = LoadConfig(
                      maxCoordsPerPartition = 43260,
                      nullPositions = false,
                      readDimensions = true,
                      roundTimeToClosestHour = true,
                      sharedStructure = true))

  extends RDD[(FileName, Position, Seq[Var], Seq[Double])](sc, Nil) {

  // this is constant for now, but it should rather depend on the configuration of the cluster
  private val MAX_COORDS_PER_PARTITION = loadConfig.maxCoordsPerPartition

  private val hadoopConf = new Configuration(sparkContext.hadoopConfiguration) with Serializable

  override def compute(split: Partition, context: TaskContext): Iterator[(FileName, Position, Seq[Var], Seq[Double])] = {
    val newIter: Iterator[(FileName, Position, Seq[Var], Seq[Double])] = new Iterator[(FileName, Position, Seq[Var], Seq[Double])] {

      // Data Load and Transform
      private val partition = split.asInstanceOf[LoadNetCdfPartition]

      private var ncDataset = openNcDatasetInHDFS(partition.fileName, hadoopConf)

      // convert end to extent
      private val extent = partition.end.zip(partition.origin).map {case (e, o) => e - o}

      // read Dimensions Values from Variable Slice if needed
      private val dimensionVals =
        if (loadConfig.readDimensions) {
        partition.dimVariables
          .zipWithIndex
          .map { case (dimName: Var, dimIndex: Int) => {
            val dimVariable = ncDataset.findVariable(dimName)
            val dimValues = dimVariable
              .read(Array(partition.origin(dimIndex)), Array(extent(dimIndex)))
              .get1DJavaArray(classOf[Double])
              .asInstanceOf[Array[Double]]
            // parse Timestamps if needed
            if (dimName.contains("time")) {
              val unitString = dimVariable.findAttribute("units").getStringValue
              val timestampConverter: Double => Instant = timeStringToInstant(unitString)
              dimValues.map(timeOffset => instantToDouble(timestampConverter(timeOffset), loadConfig.roundTimeToClosestHour))
            } else {
              dimValues
            }
          }}
      } else {
        Seq()
      }

      // Load Positions from Variable Slice
      private val newPos = partition.origin.clone()
      private val end = partition.end
      private val ncPositions = (0 until extent.product).map(_ => {
        val pos = newPos.clone()
        incr(newPos, partition.origin, end)
        pos
      })

      // read Data from Variable Slice
      private val data = partition
        .dataVariables
        .flatMap(variableName => {

          val ncVariable = ncDataset.findVariable(variableName)

          // Load Variable Slice
          val ncValues = ncVariable.read(partition.origin, extent)
            .get1DJavaArray(classOf[Double])
            .asInstanceOf[Array[Double]]

          ncValues.zipWithIndex
            .map { case (value, index) => (index, (variableName, value))}
        })
        // Transpose Grouping to Position (Group by Index -> Zip Positions)
        .groupBy(_._1)
        .toSeq
        .map { case (index, indexVarValueSeq) => {
          (ncPositions(index), indexVarValueSeq.map(_._2))
        }}
        // Transform to Output Format and Add Dimension Variables
        .map { case (pos, varValueSeq) => {
          val (varSeq, valueSeq) = varValueSeq.unzip
          // Only Add Dimension Variables if needed
          val posDimVals: Seq[Double] =
            if (loadConfig.readDimensions) {
            pos.zipWithIndex.map { case (dimPos, dimIndex) => {
              val normalizedDimPos = dimPos - partition.origin(dimIndex)
              dimensionVals(dimIndex)(normalizedDimPos)
            }}
          } else {
            Seq()
          }
          // Only Add Position if needed
          val position =
            if (loadConfig.nullPositions) {
            null
          } else {
            pos
          }

          (partition.fileName, position, partition.dimVariables ++ varSeq, posDimVals ++ valueSeq)
        }}

      // Iterator Functionality
      private var index = 0

      override def hasNext: Boolean = index < data.length

      override def next(): (FileName, Position, Seq[Var], Seq[Double]) = {
        if (!hasNext)
          throw new NoSuchElementException("No more Elements in this Partition.")
        val output = data(index)
        index += 1

        if (!hasNext) {
          close()
        }

        output
      }

      context.addTaskCompletionListener[Unit] { context =>
        close()
      }

      // Close File
      private def close(): Unit = {
        if (ncDataset == null)
          return
        ncDataset.close()
        ncDataset = null
      }

    }

    new InterruptibleIterator(context, newIter)
  }

  // Increment Position until Dimension border like a Counter
  private def incr(pos: Array[Int], origin: Array[Int], end: Array[Int]): Unit = {
    var digit = end.length - 1
    while (digit >= 0) {
      if (end(digit) < 0) {
        pos(digit) = -1
      }                               // whatever
      pos(digit) += 1
      if (pos(digit) < end(digit)) {
        return                        // normal exit
      }
      pos(digit) = origin(digit)      // else, carry
      digit -= 1
    }
  }

  override protected def getPartitions: Array[Partition] = {

    if (loadConfig.sharedStructure) {
      // Read Dimensions from first specified Data-Variable, assume the same Dimension for each Data-Variable
      // Also assume that each File shares this Structure
      val (variableDimensionShape, variableDimensionNames): (Seq[Int], Seq[String]) = {
        extractDimShapeAndDimVars(fileNames.head)
      }

      val ncFileBlock = NetCDFBlock(Seq.fill(variableDimensionShape.length){0}, variableDimensionShape)
      // Partition Data block of Dataset
      val ncBlocksPartitioned = splitPartitionToMaxSize(MAX_COORDS_PER_PARTITION, ncFileBlock)

      // Map Data block slices to Partitions
      fileNames
        .flatMap(fileName => {
          ncBlocksPartitioned.map(singleBlock => {
            (fileName, singleBlock)
          })
        })
        .zipWithIndex
        .map { case ((fileName: FileName, singleBlock: NetCDFBlock), index: Int) => {
          new LoadNetCdfPartition(index, fileName, dataVariables, variableDimensionNames, singleBlock.origin.toArray, singleBlock.end.toArray)
        }}
        .toArray
    }
    // TODO: Splitting should also be supported for the Case of varying Dimension Lengths
    else {
      fileNames
        .map(file => (file, extractDimShapeAndDimVars(file)))
        .zipWithIndex
        .map{ case ((file: FileName, (fileVariableDimShape: Seq[Int], fileVariableDimNames: Seq[String])), index: Int) => {
          new LoadNetCdfPartition(index, file, dataVariables, fileVariableDimNames, Array.fill(fileVariableDimShape.length){0}, fileVariableDimShape.toArray)
        }}
        .toArray
    }
  }

  private def extractDimShapeAndDimVars(file: FileName): (Seq[Int], Seq[String]) = {
    val fileData = openNcDatasetInHDFS(file, hadoopConf)
    val fileVar = fileData.findVariable(dataVariables.head)
    val dimShape = fileVar.getShape
    // Only load Dimensions if needed
    val dimNames =
      if (loadConfig.readDimensions) {
        JavaConverters.asScalaBuffer(fileVar.getDimensions).map(_.getFullName)
      } else {
        Seq()
      }
    if (fileData != null) {
      fileData.close()
    }
    (dimShape.toSeq, dimNames)
  }

  // Inspired by Justus Henneberg @Northlight
  private def splitPartitionToMaxSize(maxElemsPerPartition: Int, block: NetCDFBlock): Seq[NetCDFBlock] = {
    // val maxElemsPerBlock = sdiv(maxElemsPerPartition, dataVariables.length)
    val maxElemsPerBlock = maxElemsPerPartition
    // Order by descending Extent, but always put the fastest varying Dimension last
    val (reorderedExtents, reorderedDimensions) = {
      val rest :+ fastestVaryingDimension = block.end.zipWithIndex
      val sortedRest = rest.sortBy(_._1)(Ordering[Int].reverse)
      val reordered = sortedRest :+ fastestVaryingDimension
      reordered.unzip
    }

    // Sizes of Data block cumulated along Dimensions
    val cumulativeExtents = reorderedExtents.scanRight(1)(_ * _)
    // First Dimension that is too large
    val splitIndex = cumulativeExtents.indexWhere(_ <= maxElemsPerBlock) - 1

    // No Split needed, no Dimension is too large
    if (splitIndex < 0) {
      return Seq(block)
    }

    // Fully partition all Dimensions that are too large
    val fullySplit = reorderedDimensions.take(splitIndex).foldLeft(Seq(block)) { (blocks, dim) =>
      blocks.flatMap(b => splitBlockAlongDim(b, dim, 1))
    }

    // Block-partition first Dimension that is small enough
    val splitDimension = reorderedDimensions(splitIndex)
    // How many Data blocks of last fitting Dimension (cumSize <= maxElemsPerBlock) fit into one Partition
    val sliceSizeAlongSplitDimension = maxElemsPerBlock / cumulativeExtents(splitIndex + 1)
    fullySplit.flatMap(b => splitBlockAlongDim(b, splitDimension, sliceSizeAlongSplitDimension))
  }

  private def splitBlockAlongDim(block: NetCDFBlock, dimension: Int, dimensionSliceSize: Int): Seq[NetCDFBlock] = {
    val dimensionSize = block.end(dimension)
    val numPartitions = sdiv(dimensionSize, dimensionSliceSize)
    (0 until numPartitions).map(i => i * dimensionSliceSize).map { offset =>
      NetCDFBlock(
        // Start (inclusive)
        block.origin.updated(dimension, block.origin(dimension) + offset),
        // End (exclusive)
        block.end.updated(dimension, dimensionSize.min(block.origin(dimension) + dimensionSliceSize + offset))
      )
    }
  }
}
