package bachelorthesis

import bachelorthesis.utilsExternal.{FileName, Position, Var}
import bachelorthesis.utilsInternal.openNcDatasetInHDFS
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import ucar.nc2.Variable

/** Naive Implementation of Gather Join, taking a RDD containing the Join Positions as Input
 *  Unnecessary File Opens and Closes*/
@deprecated
class NaiveGatherJoinRDD[V](dataToLoad: RDD[(FileName, Position, Seq[Var], V)])
  extends RDD[(FileName, Position, (Seq[Var], Seq[Double]), V)](dataToLoad) {

  private val hadoopConf = new Configuration(sparkContext.hadoopConfiguration) with Serializable

  override def compute(split: Partition, context: TaskContext): Iterator[(FileName, Position, (Seq[Var], Seq[Double]), V)] = {
    val toLoadIterator = firstParent[(FileName, Position, Seq[Var], V)].iterator(split, context)

    val newIter = toLoadIterator.map { case (fileName: FileName, position: Position, variables: Seq[Var], passThroughVals: V) => {

      val ncDataset = openNcDatasetInHDFS(fileName, hadoopConf)

      // Read Position from every Variable of NetCdf File
      val valuesPerVariable = variables.map(variableName => {

        val ncVariable: Variable = ncDataset.findVariable(variableName)
        ncVariable.read(position, Array.fill(position.length) {1}).getDouble(0)
      })

      // Close File
      if (ncDataset != null) {
        ncDataset.close()
      }

      (fileName, position, (variables, valuesPerVariable), passThroughVals)
    }}

    new InterruptibleIterator(context, newIter)
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[(FileName, Position, Seq[Var], V)].partitions
  }
}
