package org.ruivieira.als.streaming

import breeze.linalg.DenseVector
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ruivieira.als.streaming

class SGD(val model: StreamingALSModel, val params: Parameters)
  extends Serializable {

  private def calculateBias(gamma: Double,
                            error: Double,
                            lambda: Double,
                            features: Factor): Float = {
    (gamma * (error - lambda * features.bias)).toFloat
  }

  private def gradientStep(rating: Double,
                           userFeatures: Factor,
                           prodFeatures: Factor,
                           bias: Double,
                           factorGamma: Double,
                           biasGamma: Double,
                           lambda: Double): (Factor, Factor) = {

    val predicted = StreamingALSModel.calculateRating(userFeatures,
      prodFeatures,
      bias,
      params)
    val epsilon = rating - predicted

    val userGradients = (0 until userFeatures.features.length).map { i =>
      (factorGamma * (prodFeatures.features(i) * epsilon - lambda * userFeatures
        .features(i))).toFloat
    }

    val productGradients = (0 until userFeatures.features.length).map { i =>
      (factorGamma * (userFeatures.features(i) * epsilon - lambda * prodFeatures
        .features(i))).toFloat
    }

    val userBiasGrad =
      calculateBias(biasGamma, epsilon, lambda, userFeatures)
    val prodBiasGrad =
      calculateBias(biasGamma, epsilon, lambda, prodFeatures)

    (Factor(userBiasGrad, DenseVector[Float](userGradients.toArray)),
      Factor(prodBiasGrad, DenseVector[Float](productGradients.toArray)))
  }

  def train(ratings: RDD[Rating[Long]], nRatings: Long): StreamingALSModel = {

    var userFeatures = model.userFactors
    var prodFeatures = model.productFactors
    val globalBias = model.globalBias

    val rank = params.rank

    for (i <- 0 until params.iterations) {
      val _factorGamma = params.factorGamma * math.pow(params.rate, i)
      val _biasGamma = params.biasGamma * math.pow(params.rate, i)
      val gradients = ratings
        .map(r => (r.user, r))
        .join(userFeatures)
        .map {
          case (user, (rating, uFeatures)) =>
            (rating.item, (user, rating.rating, uFeatures))
        }
        .join(prodFeatures)
        .map {
          case (item, ((user, rating, uFeatures), pFeatures)) =>
            val step = gradientStep(rating = rating,
              userFeatures = uFeatures,
              prodFeatures = pFeatures,
              bias = globalBias,
              factorGamma = _factorGamma,
              biasGamma = _biasGamma,
              lambda = params.lambda)
            ((user, step._1), (item, step._2))
        }
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val userGradients =
        gradients
          .map(_._1)
          .aggregateByKey(
            streaming.Factor(0.0f, DenseVector.zeros[Float](rank)))(
            seqOp = (base, example) => base += example,
            combOp = (a, b) => a += b
          )
      val prodGradients =
        gradients
          .map(_._2)
          .aggregateByKey(Factor(0.0f, DenseVector.zeros[Float](rank)))(
            seqOp = (base, example) => base += example,
            combOp = (a, b) => a += b
          )
      userFeatures = userFeatures.join(userGradients).map { case (user, (base, gradient)) =>
          val b = base.divideAndAdd(gradient, nRatings)
        (user, b)
      }

      prodFeatures = prodFeatures.join(prodGradients).map {
        case (user, (base, gradient)) =>
          val b = base.divideAndAdd(gradient, nRatings)
          (user, b)

      }
    }
    StreamingALSModel(
      parameters = params,
      userFactors = userFeatures,
      productFactors = prodFeatures,
      globalBias = globalBias,
      totalRating = model.totalRating
    )
  }
}
