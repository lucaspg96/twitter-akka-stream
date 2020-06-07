package twitter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{FilterQuery, Logger, Status, StatusAdapter, TwitterStreamFactory}

object TwitterSource {

  private val log = Logger.getLogger(this.getClass)

  private val config = new ConfigurationBuilder()
    .setDebugEnabled(true)
    .setOAuthConsumerKey(TwitterAuthConfig.consumerKey)
    .setOAuthConsumerSecret(TwitterAuthConfig.consumerSecret)
    .setOAuthAccessToken(TwitterAuthConfig.accessToken)
    .setOAuthAccessTokenSecret(TwitterAuthConfig.accessTokenSecret)
    .build()

  private val stream = new TwitterStreamFactory(config).getInstance()

  def createSource(searchText: Option[String] = None,geolocated: Boolean = false)
                  (implicit system: ActorSystem, materializer: ActorMaterializer): (Source[Status, NotUsed], () => Unit) = {
    val (actorRef, publisher) = Source
      .actorRef[Status](100, OverflowStrategy.fail)
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run()

    val listener = new StatusAdapter {
      override def onStatus(status: Status): Unit = {
        actorRef ! status
      }
    }

    stream.addListener(listener)
    stream.cleanUp()

    if(geolocated) {
      val query = new FilterQuery()
      query.locations(Array[Double](-180, -90), Array[Double](180, 90))
      stream.filter(query)
    }
    else searchText
      .fold(stream.sample())(search => stream.filter(search.split(" "):_*))

    log.info(s"Iniciando stream. Geolocalizada: ${geolocated} ${searchText.fold("")(s =>s"| Busca: $s")}")

    (Source.fromPublisher(publisher), stream.shutdown)
  }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("twitter-source-test")
    implicit val materializer = ActorMaterializer()
    createSource()._1.runWith(Sink.foreach(println))
  }

}
