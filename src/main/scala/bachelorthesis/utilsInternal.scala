package bachelorthesis

import bachelorthesis.utilsExternal.{FileName, Position, Var}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.{Instant, LocalDateTime, ZoneOffset}

private[bachelorthesis] object utilsInternal {

  // Division with rounding up
  def sdiv[A](dividend: A, divisor: A)(implicit ev: Integral[A]): A = {
    ev.quot(ev.minus(ev.plus(dividend, divisor), ev.fromInt(1)), divisor)
  }

  def openNcDatasetInHDFS(fileName: FileName, conf: Configuration): NetcdfDataset = {
    val path = new Path(fileName)
    val fsURI = path.getFileSystem(conf).getUri
    val raf = new HDFSRandomAccessFile(fsURI, path, conf)
    val ncFile = NetcdfFile.open(raf, path.toString, null, null)
    NetcdfDataset.wrap(ncFile, NetcdfDataset.getEnhanceAll)
  }

  def squaredEuclideanDist(first: Position, second: Position): Int = {
    first.zip(second).map { case (x: Int, y: Int) => (x - y) * (x - y)}.sum
  }

  def volumeDist(first: Position, second: Position): Int = {
    // +1 to ensure that volume doesn't turn 0
    first.zip(second).map { case (x: Int, y: Int) => math.abs(x - y) + 1}.product
  }

  def manhattanDist(first: Position, second: Position): Int = {
    first.zip(second).map { case (x: Int, y: Int) => math.abs(x - y)}.sum
  }

  def createMinPosition(positions: Seq[Position]): Position = {
    val dimension = positions.head.length
    val minPos = Array.fill(dimension)(0)
    (0 until dimension).foreach(dim => {
      val dimMin = positions.minBy(pos => pos(dim)).apply(dim)
      minPos(dim) = dimMin
    })
    minPos
  }

  def createMaxPosition(positions: Seq[Position]): Position = {
    val dimension = positions.head.length
    val maxPos = Array.fill(dimension)(0)
    (0 until dimension).foreach(dim => {
      val dimMax = positions.maxBy(pos => pos(dim)).apply(dim)
      maxPos(dim) = dimMax
    })
    maxPos
  }

  private val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .appendLiteral(" ")
    .append(DateTimeFormatter.ISO_LOCAL_TIME)
    .toFormatter()

  def timeStringToInstant(unitString: String)(offset: Double): Instant = {
    val splitted = unitString.split(" ", 3)
    val unit = splitted.head
    val timeString = splitted.last

    val baseTime = LocalDateTime.parse(timeString, formatter).atZone(ZoneOffset.UTC)

    val result = unit match {
      // extendable
      case "seconds" => baseTime.plusSeconds(offset.toLong)
      case "hours" => baseTime.plusHours(offset.toLong)
    }

    result.toInstant
  }

  // Double in Seconds since Epoch
  // Double should be able to map Seconds in Long for our purpose
  def instantToDouble(time: Instant, roundToClosestHour: Boolean): Double = {
    val result = time.getEpochSecond.toDouble
    if (roundToClosestHour) {
      (Math.round(result / 3600) * 3600).toDouble
    } else {
      result
    }
  }

  // Double in Seconds since Epoch
  def doubleToInstant(time: Double): Instant = {
    Instant.ofEpochSecond(time.toLong)
  }

  def instantToHourFilePath(time: Instant, basePath: String, ncFilePrefix: String, fileExtension: String = ".nc"): FileName = {
    val timeString = time.toString
    val year = timeString.slice(0, 4)
    val month = timeString.slice(5, 7)
    val day = timeString.slice(8, 10)
    val hour = timeString.slice(11, 13)
    f"$basePath/$year/$month/$ncFilePrefix$year$month${day}_$hour$fileExtension"
  }

  // lat between -90 and 90 in quarter steps (rounding to the closest grid point)
  def latToIndex(lat: Double): Int = {
    if (lat < -90.0 || lat > 90.0) throw new IllegalArgumentException
    Math.round((lat - 90) / 0.25).toInt * -1
  }

  // long between -180 and 179.75 in quarter steps (rounding to the closest grid point)
  def longToIndex(long: Double): Int = {
    if (long < -180 || long > 179.75) throw new IllegalArgumentException
    Math.round((long + 180) / 0.25).toInt
  }

  // lat between -90 and 90 in quarter steps
  def indexToLat25(i: Int): Double = {
    if (i < 0 || i >= 721) throw new IllegalArgumentException
    ((i * -25) + 9000).toDouble / 100
  }

  // long between -180 and 180 in quarter steps
  def indexToLong25(i: Int): Double = {
    if (i < 0 || i >= 1440 ) throw new IllegalArgumentException
    ((i * 25) - 18000).toDouble / 100
  }

  // returns Index of next smaller Element if elem not in seq, if elem smaller than every value return 0
  def binarySearch(seq: Seq[Double], elem: Double): Int = {
    var low = 0
    var high = seq.length - 1
    var result = -1

    while (low <= high) {
      val mid = low + ((high - low) / 2)
      if (elem == seq(mid)) {
        return mid
      } else if (elem < seq(mid)) {
        high = mid - 1
      }
      else {
        result = mid
        low = mid + 1
      }
    }
    result = if (result >= 0) result else 0
    result
  }

  // A and B Coefficients to compute ERA ML Model Levels
  private val HYBRID_A_COEFFICIENTS = IndexedSeq(1.0001825094223022, 2.5513030290603638, 3.884162425994873, 5.74703049659729, 8.287471771240234, 11.67619514465332, 16.107177257537842, 21.797324180603027, 28.985713958740234, 37.93247604370117, 48.9173526763916, 62.238019943237305, 78.2082290649414, 97.15581130981445, 119.42062377929688, 145.35245513916016, 175.3089828491211, 209.65375518798828, 248.75426483154297, 292.98016357421875, 342.70155334472656, 398.2874298095703, 460.10426330566406, 528.5147399902344, 603.8766784667969, 686.5420227050781, 776.8559875488281, 875.1563720703125, 981.7730407714844, 1097.0274047851562, 1221.2319946289062, 1354.6902465820312, 1497.696533203125, 1650.5359497070312, 1813.484130859375, 1986.8076171875, 2170.7637939453125, 2365.60107421875, 2571.559326171875, 2788.8697509765625, 3017.75537109375, 3258.4315185546875, 3511.10595703125, 3775.9793701171875, 4053.2105712890625, 4342.8740234375, 4644.9833984375, 4959.522216796875, 5286.44287109375, 5625.667724609375, 5977.20947265625, 6341.510498046875, 6719.40869140625, 7111.869873046875, 7519.640625, 7943.383056640625, 8383.939697265625, 8842.462890625, 9319.54150390625, 9814.33056640625, 10325.30517578125, 10850.64697265625, 11388.36474609375, 11935.8076171875, 12489.21044921875, 13045.77099609375, 13603.0, 14156.7353515625, 14703.87744140625, 15241.93603515625, 15767.18603515625, 16276.71875, 16768.0556640625, 17238.201171875, 17684.6171875, 18105.02734375, 18497.076171875, 18858.50390625, 19187.400390625, 19481.77734375, 19739.716796875, 19959.6611328125, 20139.7978515625, 20278.763671875, 20375.0859375, 20427.193359375, 20433.8984375, 20393.767578125, 20305.6640625, 20168.298828125, 19980.5556640625, 19741.298828125, 19449.3994140625, 19103.84375, 18703.583984375, 18248.31640625, 17739.3828125, 17180.263671875, 16575.3671875, 15929.37109375, 15247.57421875, 14535.888671875, 13800.546875, 13048.013671875, 12284.798828125, 11517.322265625, 10751.740234375, 9993.845703125, 9248.984375, 8521.9140625, 7816.859375, 7137.3828125, 6486.4765625, 5866.45703125, 5279.08984375, 4725.5859375, 4206.66796875, 3722.59765625, 3273.25, 2858.203125, 2476.69140625, 2127.87109375, 1810.48828125, 1523.51171875, 1265.3984375, 1034.87890625, 830.75, 651.52734375, 496.23828125, 363.4453125, 252.48046875, 162.29296875, 92.44140625, 42.80859375, 13.296875238418579, 1.878906488418579, 0.0)
  private val HYBRID_B_COEFFICIENTS = IndexedSeq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.9099999803984247E-8, 3.3994501080769624E-6, 1.555435051159293E-5, 4.1635000343376305E-5, 8.541814895579591E-5, 1.552458488731645E-4, 2.694785434869118E-4, 4.5096750545781106E-4, 7.256266253534704E-4, 0.0011212517274543643, 0.0016723217559047043, 0.0024244810920208693, 0.003414038917981088, 0.0046743841376155615, 0.006255595711991191, 0.008197418414056301, 0.010533741209656, 0.013310825452208519, 0.016567040234804153, 0.02033664844930172, 0.024659182876348495, 0.029569808393716812, 0.03510124795138836, 0.04128718003630638, 0.0481604877859354, 0.05575071461498737, 0.06408833339810371, 0.07320328056812286, 0.08312202244997025, 0.09387370198965073, 0.10548315942287445, 0.11797638982534409, 0.13138050958514214, 0.14571896195411682, 0.16101772338151932, 0.17729993164539337, 0.19459033012390137, 0.21291203796863556, 0.2322884351015091, 0.2527429461479187, 0.27429795265197754, 0.296976238489151, 0.3207687735557556, 0.34559664130210876, 0.37130875885486603, 0.3977440446615219, 0.4247579872608185, 0.45219725370407104, 0.4799018055200577, 0.5077097564935684, 0.5354601740837097, 0.5629966557025909, 0.5901701152324677, 0.6168419420719147, 0.6428858935832977, 0.6681894958019257, 0.692656010389328, 0.7162038683891296, 0.7387676537036896, 0.7602970600128174, 0.7807571589946747, 0.8001264035701752, 0.8183960616588593, 0.8355686068534851, 0.8516564667224884, 0.8666805326938629, 0.8806684017181396, 0.8936540186405182, 0.9056743383407593, 0.9167719185352325, 0.9269882142543793, 0.9363701641559601, 0.9449619948863983, 0.9528069794178009, 0.9599506258964539, 0.9664325714111328, 0.9722959101200104, 0.9775750041007996, 0.9823067486286163, 0.9865207076072693, 0.9902417659759521, 0.9934932589530945, 0.9963163137435913, 0.9988150596618652)

  def computePressureFromSP(level: Int, surfacePressure: Double): Double = {
    HYBRID_A_COEFFICIENTS(level) + surfacePressure * HYBRID_B_COEFFICIENTS(level)
  }
}
