package org.ruivieira.als.streaming

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD

class StreamingALS(params: Parameters) {

  protected var model: Option[StreamingALSModel] = None

  def train(ratings: RDD[Rating[Long]]): StreamingALSModel = {

    val (_model, totalRatings) = StreamingALSModel.initialize(
      ratings = ratings,
      parameters = params,
      model = model
    )

    val sgd = new SGD(_model, params)
    model = Some(sgd.train(ratings = ratings, nRatings = totalRatings))
    model.get
  }

  def predictAll(data: RDD[(Long, Long)]): RDD[Rating[Long]] = {
    model.get.predict(data)
  }

}
