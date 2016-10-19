package sfdc.kbrecommend

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD
import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

case class ECommAlgorithmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]
) extends Params

case class PageModel(

page: Page
)

case class UserModel(
item: User,
features: Option[Array[Double]],
count: Int
)

case class ScoreDescription(score: Double, basedon: Set[String])

case class ProductModel(
  item: Item,
  features: Option[Array[Double]], // features by ALS
  count: Int // popular count for default score
)

class KBModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val productModels: Map[Int, ProductModel],
  val pageModels: Map[Int, Page],
  val userStringIntMap: BiMap[String, Int],
  val itemStringIntMap: BiMap[String, Int],
  val pageStringIntMap: BiMap[String, Int]
) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse
  @transient lazy val pageIntStringMap = pageStringIntMap.inverse


  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.size}]" +
    s"(${productModels.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2).toString}...)]" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

class KBAlgorithm(val ap: ECommAlgorithmParams)
  extends P2LAlgorithm[PreparedData, KBModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): KBModel = {
    require(!data.viewEvents.take(1).isEmpty,
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)
    val pageStringIntMap = BiMap.stringInt(data.pages.keys)
    logger.info(s"users: ${data.users.count()} itemonpage in ecom: ${data.itemOnPage.count()} items: ${data.items.count()}")

  

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data
    )

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and item ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    val userFeatures = m.userFeatures.collectAsMap.toMap

    // convert ID to Int index
    val items = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }

    val pages = data.pages.map { case(id, page) =>
    (pageStringIntMap(id), page)
    }

    val pageModels : Map[Int, Page] = 
    pages.collectAsMap.toMap

    // join item with the trained productFeatures
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
      items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val popularCount = trainDefault(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data
    )

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features,
          // NOTE: use getOrElse because popularCount may not contain all items.

          count = popularCount.getOrElse(index, 0)
          
        )
        
        (index, pm)
      }

    new KBModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      pageModels = pageModels,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      pageStringIntMap = pageStringIntMap
    )
  }

  /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRating(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 1)
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }
      .cache()

    mllibRatings
  }

   /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRatingPages(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.viewPageEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 1)
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }
      .cache()

    mllibRatings
  }

  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for item.
    */
  def trainDefault(
    userStringIntMap: BiMap[String, Int],
    itemStringIntMap: BiMap[String, Int],
    data: PreparedData): Map[Int, Int] = {
    // count number of buys
    // (item index, count)
    val buyCountsRDD: RDD[(Int, Int)] = data.upVoteEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        (uindex, iindex, 1)
      }
      .filter { case (u, i, v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    buyCountsRDD.collectAsMap.toMap
  }


  //needs predict because framework expects it.
  def predict(model: KBModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val productModels = model.productModels
    val pageModels = model.pageModels


    

    // convert whiteList's string ID to integer index
    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.flatMap(model.itemStringIntMap.get(_))
    )

    val finalBlackList: Set[Int] = genBlackList(query = query)
      // convert seen Items list from String ID to interger Index
      .flatMap(x => model.itemStringIntMap.get(x))

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex =>
        userFeatures.get(userIndex)
      }
   
   val recentPageViews = getRecentPageViews(query)



    val topScores: Array[(Int, ScoreDescription)] = if (userFeature.isDefined) {
      // the user has feature vector
      logger.info(s"Yes userFeature found for user ${query.user} ${userFeature.get}")
      val basedon = "knownuser"
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList,
        pageModels = pageModels,
        recentPageViews = recentPageViews,
        model = model
      )
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      logger.info(s"No userFeature found for user ${query.user}.")

      // check if the user has recent events on some items
      val recentItems: Set[String] = getRecentItems(query)
      val recentList: Set[Int] = recentItems.flatMap (x =>
        model.itemStringIntMap.get(x))

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        // productModels may not contain the requested item
        .map { i =>
          productModels.get(i).flatMap { pm => pm.features }
        }.flatten

      if (recentFeatures.isEmpty) {
        logger.info(s"No features vector for recent items ${recentItems}.")
        val basedon = "knownuser"
        predictDefault(
          productModels = productModels,
          query = query,
          recentPageViews = recentPageViews,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      } else {
      val basedon = "knownuser"
        predictSimilar(
          recentFeatures = recentFeatures,
          productModels = productModels,
          recentPageViews = recentPageViews,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      }
    }


 


    val itemScores = topScores.map { case (i, s) =>
      

      
      new ItemScore(
        // convert item int index back to string ID
        item = model.itemIntStringMap(i),
        score = s.score,
        basedon = s.basedon,
        article = model.productModels(i).item.article
      )
    }
   
    new PredictedResult(itemScores)
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[String] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e : Throwable=> {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("items")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Throwable =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.getOrElse(Set[String]()) ++ seenItems ++ unavailableItems
  }

  /** Get recent events of the user on items for recommending similar items */
  def getRecentItems(query: Query): Set[String] = {
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Throwable =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Throwable => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

/** Get recent pageviews to boost articles that are popular over both pages */
  def getRecentPageViews(query: Query): Set[String] = {
    // get latest 10 user view item events
    val recentPEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(Seq("pageview")),
        targetEntityType = Some(Some("page")),
        limit = Some(3),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Throwable =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentPEvents.map { event =>
      try {
      logger.info(s"have page ${event.targetEntityId.get}")
        event.targetEntityId.get
      } catch {
        case e: Throwable => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

 /** Get upvotes for the items in the current shoppingcart */
  def getUpvotesForShoppingCart(query: Query): Set[String] = {
    // get all upvotes
    val recentEvents = try {
      LEventStore.find(
        appName = ap.appName,
        // entityType should prevent table scan
        entityType = Some("user"),
        // entityId = query.user,
        eventNames = Some(Seq("upvote")),
        targetEntityType = Some(Some("item")),
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Throwable =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
      val eventcart : Option[Set[String]] =  event.properties.getOpt[Set[String]]("cart")
      val sofun : Set[String] = eventcart.getOrElse(Set("ppp"))

      if(sofun != "[]" && !sofun.intersect(query.cart.getOrElse(Set("xxxx"))).isEmpty && query.page == Some(event.properties.get[String]("page")))
        {event.targetEntityId.get}
        else {""}
      } catch {
        case e: Throwable => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }



   /** Get upvotes for the items recently viewed */
  def getUpvotesForRecentViews(query: Query, pageviews: Set[String]): Set[String] = {
    // get all upvotes
    val recentUEvents = try {
      LEventStore.find(
        appName = ap.appName,
        // entityType should prevent table scan
        entityType = Some("user"),
        // entityId = query.user,
        eventNames = Some(Seq("upvote")),
        targetEntityType = Some(Some("item")),
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when reading upvotes." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Throwable =>
        logger.error(s"Error when read upvotes: ${e}")
        throw e
    }




    val recentvItems: Set[String] = recentUEvents.map { event =>
      try {
      //val eventcart : Option[Set[String]] =  event.properties.getOpt[Set[String]]("cart")
      //val sofun : Set[String] = eventcart.getOrElse(Set("ppp"))

      
      
      if(pageviews != "[]" && pageviews.contains((event.properties.get[String]("page"))))
        {
        
        logger.info(s" upvote on item ${event.targetEntityId.get} ")
        event.targetEntityId.get}
        else {""}
      } catch {
        case e: Throwable => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentvItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    productModels: Map[Int, ProductModel],
    query: Query,
    recentPageViews: Set[String],
    pageModels: Map[Int, Page],
    whiteList: Option[Set[Int]],
    blackList: Set[Int],
    model: KBModel
  ): Array[(Int, ScoreDescription)] = {
    
    val upvoteForCart = getUpvotesForShoppingCart(query)
    val upvoteForRecentViews = getUpvotesForRecentViews(query, recentPageViews)
    logger.info(s"cartfun ${upvoteForCart}")

    val indexScores: Map[Int, ScoreDescription] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          onpage = query.page,
          categories = query.categories,
          whiteList = whiteList,
          recentPageViews = recentPageViews,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, pm.features.get)
        // here we have the item, which contains the page it is viewed on and the masterarticle
        // we need to get the master article, and see if this current master article has also had
        // upvotes on pages that are in the user shoppingbasket
        
        // we have a cart with pagestrings
        // we have an item with a main article
        // we need to loop trough the page of the main article and see if the cart is mentioned there.

   
   // booster for pages/products that are in shopping cart and upvoted on productpage
    var itemrectype : String = ""
    var booster : Double = 1
    var scoreReason : Set[String] = Set("UserFeatures")
    var page : String = query.page.getOrElse("") 
    if(pm.item.page == page && page != ""){
    scoreReason += "CurrentPage"
    }
    if(recentPageViews.contains(pm.item.page)  && pm.item.page != page){
      itemrectype = "Recent"
      scoreReason += "RecentViews"
      logger.info(s"recentpage negative boosting for this")
      booster -= 0.2
    }

    
     val currentmainarticle = pm.item.article


    if(itemrectype =="Recent"){
     logger.info(s"is recent")
     val nodups = pageModels.filter{ case(x, pagem) =>
     
       model.pageIntStringMap(x) == page &&
       pagem.upvoted.get.contains(currentmainarticle)
      }
     .foreach{ pb =>
     
         booster -= 50
          }
     }
    


    if(query.cart != None){

    var cart : Set[String] = query.cart.get
    if(cart.contains(pm.item.page)  && pm.item.page != page){
      itemrectype = "Cart"
      scoreReason += "InCart"
      logger.info(s"incart negative boosting for this")
      booster -= 0.1
    }
  
     

    
 
     val incartboost = pageModels.filter{ case(x, pagem) =>
       query.cart.get.contains(model.pageIntStringMap(x)) &&
       pagem.upvoted.get.contains(currentmainarticle)
        }
        .foreach{ pb =>
     //superboost for articles that are liked on multiple shopitem productpages
           logger.info(s"boosting for this")
            if(itemrectype=="Cart"){
     //already on this page
               booster -= 50
            } else {
                scoreReason += "CartProduct"
     //val p : Double = booster*1.2
                booster += 0.4
          }
         }


   //booster for articles that were upvoted on this page by users when they had the same items in shoppingcart
   // we're not storing this in the model, so on heroku with postgress this will be slowing things down

    if(upvoteForCart.contains(pm.item.article)){
    scoreReason += "CartSimilar"
logger.info(s"shoppingcart boosting for this")
    booster += 0.8
    }


}

if(upvoteForRecentViews.contains(pm.item.article)){
    scoreReason += "RecentViewsSimilar"
logger.info(s"recentview boosting for this")
    booster += 0.4
    }

val boosteds = s*booster
    
 // may customize here to further adjust score
        (i, ScoreDescription(boosteds, scoreReason))
      }
      .filter(_._2.score > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, ScoreDescription), Double](_._2.score).reverse
    val topScores = getTopN(indexScores, query.num, "knownuser")(ord).toArray
    
    topScores  
  
  }

  /** Default prediction when know nothing about the user */
  def predictDefault(
    productModels: Map[Int, ProductModel],
    query: Query,
    recentPageViews: Set[String],
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, ScoreDescription)] = {
    val indexScores: Map[Int, ScoreDescription] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          onpage = query.page,
          categories = query.categories,
          recentPageViews = recentPageViews,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        // may customize here to further adjust score
        (i, ScoreDescription(pm.count.toDouble, Set("default")))
      }
      .seq

    val ord = Ordering.by[(Int, ScoreDescription), Double](_._2.score).reverse
    val topScores = getTopN(indexScores, query.num, "knownuser")(ord).toArray

    topScores 
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilar(
    recentFeatures: Vector[Array[Double]],
    productModels: Map[Int, ProductModel],
    query: Query,
    recentPageViews: Set[String],
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, ScoreDescription)] = {
    val indexScores: Map[Int, ScoreDescription] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          onpage = query.page,
          recentPageViews = recentPageViews,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        val s = recentFeatures.map{ rf =>
          // pm.features must be defined because of filter logic above
          cosine(rf, pm.features.get)
        }.reduce(_ + _)
        // may customize here to further adjust score
        (i, ScoreDescription(s, Set("similar")))
      }
      .filter(_._2.score > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, ScoreDescription), Double](_._2.score).reverse
    val topScores = getTopN(indexScores, query.num, "knownuser")(ord).toArray

    topScores 
  }

  private
  def getTopN[T](s: Iterable[T], n: Int, fromp: String)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

     q.dequeueAll.toSeq.reverse
   
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
    i: Int,
    item: Item,
    onpage: Option[String],
    categories: Option[Set[String]],
    whiteList: Option[Set[Int]],
    recentPageViews: Set[String],
    blackList: Set[Int]
  ): Boolean = {
 // logger.info(s"looping items ${onpage} ${item.page}.")
    // can add other custom filtering here
    whiteList.map(_.contains(i)).getOrElse(true) &&
    (onpage.getOrElse("") == item.page || recentPageViews.contains(item.page)) &&
    !blackList.contains(i) &&
    // filter categories
    categories.map { cat =>
      item.categories.map { itemCat =>
        // keep this item if has ovelap categories with the query
        !(itemCat.toSet.intersect(cat).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    }.getOrElse(true)

  }

}