package sfdc.kbrecommend

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger

class Preparator
  extends PPreparator[TrainingData, PreparedData] {
  @transient lazy val logger = Logger[this.type]

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    logger.info(s"users: ${trainingData.users.count()} itemonpage full: ${trainingData.itemOnPage.count()} item : ${trainingData.items.count()} ")

    new PreparedData(
      users = trainingData.users,
      items = trainingData.items.union(trainingData.itemOnPage.distinct()),
      pages = trainingData.pages,
      viewPageEvents = trainingData.viewPageEvents,
      viewEvents = trainingData.viewEvents,
      buyEvents = trainingData.buyEvents,
      itemOnPage = trainingData.itemOnPage.distinct(),
      upVoteEvents = trainingData.upVoteEvents,
      downVoteEvents = trainingData.downVoteEvents)
      
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val pages: RDD[(String, Page)],
  val viewEvents: RDD[ViewEvent],
  val viewPageEvents: RDD[ViewPageEvent],
  val buyEvents: RDD[BuyEvent],
  val itemOnPage: RDD[(String, Item)],
  val upVoteEvents: RDD[UpVoteEvent],
  val downVoteEvents: RDD[DownVoteEvent]
) extends Serializable