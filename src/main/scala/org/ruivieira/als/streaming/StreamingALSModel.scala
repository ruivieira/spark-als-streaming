package org.ruivieira.als.streaming

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD

case class StreamingALSModel(parameters: Parameters,
                             userFactors: RDD[(Long, Factor)],
                             productFactors: RDD[(Long, Factor)],
                             globalBias: Double,
                             totalRating: Long) {

  def predict(usersProducts: RDD[(Long, Long)]): RDD[Rating[Long]] = {
    val users = usersProducts.leftOuterJoin(userFactors).map {
      case (user, (product, uFeatures)) =>
        (product, (user, uFeatures))
    }
    val sc = usersProducts.sparkContext
    val _globalBias = sc.broadcast(globalBias)
    val _paramaters = sc.broadcast(parameters)
    users.leftOuterJoin(productFactors).map {
      case (product, ((user, uFeatures), pFeatures)) =>
        StreamingALSModel.predict(user,
          product,
          uFeatures,
          pFeatures,
          _globalBias.value,
          _paramaters.value)
    }
  }
}

object StreamingALSModel extends Serializable {

  def createEmptyFactors(sc: SparkContext,
                         numSlices: Int): RDD[(Long, Factor)] = {
    val factors = sc.parallelize(Seq.empty[(Long, Factor)], numSlices)
    factors

  }

  def initialize(
                  ratings: RDD[Rating[Long]],
                  parameters: Parameters,
                  model: Option[StreamingALSModel]): (StreamingALSModel, Long) = {

    val ur = ratings.map(r => (r.user, r))
    val pr = ratings.map(r => (r.item, r))
    val sc = ratings.sparkContext

    var userFeatures = if (model.isDefined) {
      model.get.userFactors
    } else {
      createEmptyFactors(sc, ratings.partitions.length)
    }

    var prodFeatures = if (model.isDefined) {
      model.get.productFactors
    } else {
      createEmptyFactors(sc, ratings.partitions.length)
    }

    userFeatures =
      ur.fullOuterJoin(userFeatures)
        .mapPartitionsWithIndex {
          case (partitionId, iterator) =>
            iterator.map {
              case (user, (_, uFeatures)) =>
                (user, uFeatures.getOrElse(Factor.createRandom(parameters)))
            }
        }

    prodFeatures =
      pr.fullOuterJoin(prodFeatures)
        .mapPartitionsWithIndex {
          case (partitionId, iterator) =>
            iterator.map {
              case (user, (_, pFeatures)) =>
                (user, pFeatures.getOrElse(Factor.createRandom(parameters)))
            }
        }

    val ratingsSum = ratings.map(_.rating).sum()
    val totalBatchRatings = ratings.count()

    val (globalBias, numExamples) = if (model.isDefined) {
      val totalRatings: Long = model.get.totalRating + totalBatchRatings
      ((model.get.globalBias * model.get.totalRating + ratingsSum) / totalRatings,
        totalRatings)
    } else {
      ((ratingsSum / totalBatchRatings).toDouble, totalBatchRatings)
    }

    val initializedModel = if (model.isDefined) {
      StreamingALSModel(
        parameters = parameters,
        userFactors = userFeatures,
        productFactors = prodFeatures,
        globalBias = model.get.globalBias,
        totalRating = model.get.totalRating
      )
    } else {
      StreamingALSModel(parameters = parameters,
        userFactors = userFeatures,
        productFactors = prodFeatures,
        globalBias = globalBias,
        totalRating = numExamples)
    }
    (initializedModel, totalBatchRatings)
  }

  def predict(user: Long,
              product: Long,
              uFeatures: Option[Factor],
              pFeatures: Option[Factor],
              globalBias: Double,
              parameters: Parameters): Rating[Long] = {
    val finalRating =
      if (uFeatures.isDefined && pFeatures.isDefined) {
        Rating[Long](user,
          product,
          StreamingALSModel.calculateRating(uFeatures.get,
            pFeatures.get,
            globalBias,
            parameters))
      } else if (uFeatures.isDefined) {
        val rating = globalBias + uFeatures.get.bias
        Rating[Long](user, product, scaleRating(rating, parameters))
      } else if (pFeatures.isDefined) {
        val rating = globalBias + pFeatures.get.bias
        Rating[Long](user, product, scaleRating(rating, parameters))
      } else {
        val rating = globalBias
        Rating[Long](user, product, scaleRating(rating, parameters))
      }
    finalRating
  }

  def calculateRating(userFeatures: Factor,
                      prodFeatures: Factor,
                      bias: Double,
                      parameters: Parameters): Float = {
    val rating = userFeatures.features.dot(prodFeatures.features) + userFeatures.bias + prodFeatures.bias + bias
    scaleRating(rating, parameters)
  }

  def scaleRating(rating: Double, parameters: Parameters): Float = {
    math
      .min(parameters.maximumRating, math.max(parameters.minimumRating, rating))
      .toFloat
  }

}
