import java.util.concurrent.ThreadLocalRandom

import akka.actor._
import akka.pattern.after
import akka.stream.{OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}


case class Token(value: String)
case class HttpGetRequest(path: String)

case class HttpResponse(body: String)


case class HttpGetRequestWithContext(request: HttpGetRequest, token: Token, response: Promise[HttpResponse])

object Entrypoint {
  // hardcoded - these would need to be dynamically acquired
  private val tokens = Vector(Token("one"), Token("two"), Token("three"), Token("four"))


  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val ec: ExecutionContext = system.dispatcher

    system.log.info("hello")

    val client = new Client()

    (1 to 10).map(_ % tokens.length).map(tokens.apply).foreach { token =>

      // @TODO improve the token mechanism, right now round robin

      val response = client.retrieveData(token)

      response.onComplete { _ => system.log.info("TOKEN={} caller received response", token.value)}

    }

  }
}

class Client()(implicit system: ActorSystem, ec: ExecutionContext) {
  private lazy val queue = Source
    .queue[HttpGetRequestWithContext](bufferSize = 128, overflowStrategy = OverflowStrategy.dropHead)
    .groupBy(maxSubstreams = Int.MaxValue, _.token.value)
    .async
    .mapAsync(1) { requestWithContext =>
      rawApiCall(requestWithContext.request, requestWithContext.token)
        .flatMap { response =>
          // we got the API response - let's send it to the caller
          // immediately, via completing promise, and then wait 2 seconds
          // before completing, which will then let in another request to
          // our substream

          requestWithContext.response.success(response)


          after(2.seconds)(Future.successful(requestWithContext.token))
        }
        .map { token =>
          system.log.info("TOKEN={} two second delay is over", token.value)
        }
    }
    .buffer(10, OverflowStrategy.backpressure)
    .mergeSubstreams
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def retrieveData(token: Token): Future[String] = {
    val response = Promise[HttpResponse]
    val requestWithContext = HttpGetRequestWithContext(HttpGetRequest("/data"), token, response)

    queue.offer(requestWithContext).onComplete {
      case Success(QueueOfferResult.Enqueued) =>
        // nothing to do, downstream will complete us

      case Success(QueueOfferResult.Dropped) =>
        response.failure(new RuntimeException("Dropped"))

      case Success(QueueOfferResult.QueueClosed) =>
        response.failure(new RuntimeException("QueueClosed"))

      case Success(QueueOfferResult.Failure(cause)) =>
        response.failure(cause)

      case Failure(cause) =>
        response.failure(cause)
    }

    response
      .future
      .map(_.body)
  }

  private def rawApiCall(request: HttpGetRequest, token: Token)(implicit system: ActorSystem): Future[HttpResponse] = {
    val delay = randomDelay()
    system.log.info("TOKEN={} dispatched api call, will take {}s", token.value, delay)

    after(delay.seconds) {
      system.log.info("TOKEN={} completed api call, waiting two seconds", token.value)

      Future.successful(HttpResponse("precious response data"))
    }
  }

  private def randomDelay(): Int = {
    val lower = 5
    val upper = 10

    lower + ThreadLocalRandom.current().nextInt( (upper - lower) + 1 )
  }
}
