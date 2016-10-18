package sfdc.kbrecommend

import org.apache.predictionio.controller.IEngineFactory
import org.apache.predictionio.controller.Engine

case class Query(
  user: String,
  num: Int,
  page: Option[String],
  categories: Option[Set[String]],
  whiteList: Option[Set[String]],
  blackList: Option[Set[String]],
  cart: Option[Set[String]]
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

case class ItemScore(
  item: String,
  score: Double,
  basedon: Set[String],
  article: String
) extends Serializable

object EmbeddedKBRecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("embedkb" -> classOf[KBAlgorithm]),
      classOf[Serving])
  }
}