package sfdc.kbrecommend

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.predictionio.controller.SanityCheck // ADDED

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
    User()
      //  User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()


  

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"), itype = "article", page="", article = entityId)
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

   
      


    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "buy", "upvote", "downvote", "viewpage")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      .cache()

val itemOnPageRDD: RDD[(String, Item)] = eventsRDD
   .filter { event => event.event == "view" }
     .map { event =>
        val unid : String = event.targetEntityId.get + event.properties.get[String]("page")
        val item = try {
          Item(
            categories = None,
            itype = "articleonpage",
            page = event.properties.get[String]("page"),
            article = event.targetEntityId.get
          )
          
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to pageview." +
              s" Exception: ${e}.")
            throw e
        }
      
      (unid, item)
      }.cache()

val seenEvents: Array[Event] = try {
        PEventStore.find(
          appName = dsp.appName,
          entityType = Some("user"),
          eventNames = Some(List("upvote")),
          targetEntityType = Some(Some("item")))(sc)
          // set time limit to avoid super long DB access

        .map { event => 

        event
      }.collect()
      } catch {
    
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }
  
def upvoted(entityId: String): Array[String] = seenEvents
        .filter { event =>  event.properties.get[String]("page") == entityId}
        .map { event => 

       
      try {
        event.targetEntityId.get
      } catch {
        case e: Throwable => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }


// create a RDD of (entityID, Page)
    val pagesRDD: RDD[(String, Page)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "page"
    )(sc).map { case (entityId, properties) =>
      val item = try {
       
        // Assume nothing.
        Page(
        
      upvoted = Some(List.fromArray(upvoted(entityId)))

        )

     

      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

/**

val pagesRDD: RDD[(String, Page)] = eventsRDD
   .filter { event => event.event == "upvote" }
     .map { event =>
        val unid : String = event.properties.get[String]("page")
        val item = try {
          Page(
            upvoted = event.targetEntityId.get
            itype = "articleonpage",
            page = event.properties.get[String]("page"),
            article = event.targetEntityId.get
          )
          
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to pageview." +
              s" Exception: ${e}.")
            throw e
        }
      
      (unid, item)
      }.cache()

**/

   val viewEventsPageSpecificRDD: RDD[ViewEvent] = eventsRDD
     .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            item = event.targetEntityId.get + event.properties.get[String]("page"),
            page = event.properties.get[String]("page"),
            t = event.eventTime.getMillis,
            article = event.targetEntityId.get
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }


    val viewEventsRDD: RDD[ViewEvent] = eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            page = event.properties.get[String]("page"),
            t = event.eventTime.getMillis,
            article = event.targetEntityId.get
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }
  
   val viewPageEventsRDD: RDD[ViewPageEvent] = eventsRDD
      .filter { event => event.event == "pageview" }
      .map { event =>
        try {
          ViewPageEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
            
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to PageViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val buyEventsRDD: RDD[BuyEvent] = eventsRDD
      .filter { event => event.event == "buy" }
      .map { event =>
        try {
          BuyEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            page = event.properties.get[String]("page"),
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to BuyEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

   val upVoteEventsRDD: RDD[UpVoteEvent] = eventsRDD
      .filter { event => event.event == "upvote" }
      .map { event =>
        try {
          UpVoteEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            page = event.properties.get[String]("page"),
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to upvoteEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

  val downVoteEventsRDD: RDD[DownVoteEvent] = eventsRDD
      .filter { event => event.event == "downvote" }
      .map { event =>
        try {
          DownVoteEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            page = event.properties.get[String]("page"),
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to BuyEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      pages = pagesRDD,
      viewEvents = viewEventsRDD.union(viewEventsPageSpecificRDD),
      viewPageEvents = viewPageEventsRDD,
      itemOnPage = itemOnPageRDD,
      buyEvents = buyEventsRDD,
      upVoteEvents = upVoteEventsRDD,
      downVoteEvents = downVoteEventsRDD
    )
  }
}

case class Page(upvoted: Option[List[String]]) 

//{override def toString = {
//    s"upvoted: [(${upvoted.take(3).toList}...)]" }}

case class User()

case class ItemOnPage(categories: Option[List[String]], itype: String, page:String, article:String)

case class Item(categories: Option[List[String]], itype: String, page:String, article:String)

case class ViewEvent(user: String, item: String, t: Long, page: String,article:String)

case class ViewPageEvent(user: String, item: String, t: Long)

case class BuyEvent(user: String, item: String, t: Long, page: String)

case class UpVoteEvent(user: String, item: String, t: Long, page: String)

case class DownVoteEvent(user: String, item: String, t: Long, page: String)


class TrainingData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val pages: RDD[(String, Page)],
  val viewEvents: RDD[ViewEvent],
  val viewPageEvents: RDD[ViewPageEvent],
  val buyEvents: RDD[BuyEvent],
  val upVoteEvents: RDD[UpVoteEvent],
  val itemOnPage: RDD[(String, Item)],
  val downVoteEvents: RDD[DownVoteEvent]
) extends Serializable with SanityCheck  {  
  override def toString = {
    s"users: [${users.count()} (${users.take(3).toList}...)]" +
    s"items: [${items.count()} (${items.take(3).toList}...)]" +
    s"pages: [${pages.count()} (${pages.take(3).toList}...)]" +
    s"items: [${itemOnPage.count()} (${itemOnPage.take(3).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
    s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
override def sanityCheck(): Unit = {
    println(toString())
    // add your other checking here
    require(!items.take(1).isEmpty, s"items cannot be empty!")
  }
}
