package org.ruivieira.als.streaming

import breeze.linalg.DenseVector

import scala.util.Random

object Factor {

  private val random = new Random()

  private def scaled(data: Double,
                     max: Double,
                     min: Double,
                     rank: Int): Float = {
    (math.sqrt(random.nextFloat() * (max - min) + min) / rank).toFloat
  }

  def createRandom(parameters: Parameters): Factor = {

    val bias = scaled(random.nextDouble,
      parameters.maximumRating,
      parameters.minimumRating,
      parameters.rank)

    Factor(bias,
      DenseVector.fill[Float](parameters.rank)(
        scaled(random.nextDouble,
          parameters.maximumRating,
          parameters.minimumRating,
          parameters.rank)))
  }

}

case class Factor(var bias: Float, features: DenseVector[Float])
  extends Serializable {

  def +=(other: Factor): Factor =
    Factor(bias + other.bias, features + other.features)

  def divideAndAdd(other: Factor, scale: Long): Factor = {
    bias += other.bias / scale
    features += other.features / scale.toFloat
    this
  }
}
