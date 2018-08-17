package org.ruivieira.als.streaming

case class Parameters(rank: Int = 20,
                      lambda: Double = 10.0,
                      iterations: Int = 10,
                      minimumRating: Float = 0.5f,
                      maximumRating: Float = 5.0f,
                      factorGamma: Double = 1.0,
                      biasGamma: Double = 1.0,
                      rate: Double = 0.9) {}
