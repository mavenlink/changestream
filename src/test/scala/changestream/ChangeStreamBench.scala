package changestream

import akka.actor.Props
import akka.testkit.TestActorRef
import changestream.actors._
import changestream.actors.EncryptorActor.{Ciphertext, Plaintext}
import changestream.events._
import changestream.helpers.{BenchBase, Fixtures}
import spray.json.{JsNumber, JsObject, JsString}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ChangeStreamBench extends BenchBase {
  val ITERATIONS = 100000

  def queryAndWait(sql: String): Unit = Await.result(connection.sendQuery(sql), connectionTimeout milliseconds)

// Run this at your own risk ;)
//  performance of "SnsActor" in {
//    val testKit = new TestKit(ActorSystem("changestream_bench", ConfigFactory.load("test.conf")))
//    val actor = TestActorRef(
//      testKit.system.actorOf(
//        Props(new SnsActor(testConfig.getConfig("changestream.aws"))), "test"
//      )
//    )
//
//    measure method "send" in {
//      (1 to 10).foreach({ idx =>
//        actor ! "{json: true}"
//      })
//      testKit.receiveN(10, 10 seconds)
//    }
//  }

  performance of "EncryptorActor" in {
    val actor = getProbedActorOf[EncryptorActor](classOf[EncryptorActor], "changestream.encryptor")
    def sourceObject(a: String, b: String) = JsObject(
      "no_encrypt" -> JsString(a),
      "no_encrypt_hash" -> JsObject("a" -> JsNumber(1), "b" -> JsNumber(2)),
      "do_encrypt" -> JsString(b),
      "do_encrypt_hash" -> JsObject("a" -> JsNumber(1), "b" -> JsNumber(2))
    )
    val CRYPT_FIELDS = Seq("do_encrypt", "do_encrypt_hash")

    measure method "encrypt" in {
      (1 to ITERATIONS).foreach({ idx =>
        actor ! Plaintext(sourceObject(s"hello world ${idx}", s"hello world ${idx}"), CRYPT_FIELDS)
      })
      probe.receiveN(ITERATIONS)
    }

    measure method "decrypt" in {
      (1 to ITERATIONS).foreach({ idx =>
        actor ! Ciphertext(sourceObject("IqIDXjLEQXDY7DCpRFIWzA==", "IqIDXjLEQXDY7DCpRFIWzA=="), CRYPT_FIELDS)
      })
      probe.receiveN(ITERATIONS)
    }
  }

  performance of "JsonFormatterActor" in {
    val actor = getProbedActorOf[JsonFormatterActor](classOf[JsonFormatterActor])
    val (mutation, _, _) = Fixtures.mutationWithInfo("insert", 1, 1, false, true, 1)

    measure method "format" in {
      (0 to ITERATIONS).foreach({ idx =>
        actor ! mutation
      })
      probe.receiveN(ITERATIONS)
    }
  }

  performance of "ColumnInfoActor" in {
    val actor = getProbedActorOf[ColumnInfoActor](classOf[ColumnInfoActor], "changestream.mysql")
    val (mutation, _, _) = Fixtures.mutationWithInfo("insert", 1, 1, false, true, 1)

    measure method "mutation" in {
      (1 to ITERATIONS).foreach({ idx =>
        actor ! mutation
      })
      probe.receiveN(ITERATIONS)
    }
  }

  performance of "TransactionActor" in {
    val actor = TestActorRef(Props(classOf[TransactionActor], maker))
    val (mutation, _, _) = Fixtures.mutationWithInfo("insert", 1, 1, false, true, 1)

    measure method "mutation" in {
      (0 to ITERATIONS).foreach({ idx =>
        actor ! BeginTransaction
        actor ! mutation
        actor ! mutation
        actor ! CommitTransaction
      })
      probe.receiveN(ITERATIONS * 2)
    }
  }

// Not currently working
//  performance of "ChangeStream" in {
//    val app = new Thread {
//      override def run = ChangeStream.main(Array())
//    }
//    ChangeStreamEventListener.setEmitterLoader(_ => probe.ref)
//    System.setProperty("config.resource", "test.conf")
//    System.setProperty("MYSQL_SERVER_ID", Random.nextLong.toString)
//    ConfigFactory.invalidateCaches()
//    app.start()
//
//    measure method "mutation" in {
//      (1 to 100).foreach({ idx =>
//        queryAndWait(s"""INSERT INTO changestream_test.users VALUES (NULL, "peter", "hello", 10, "I Am Peter")""")
//      })
//      probe.receiveN(100)
//    }
//
//    app.interrupt()
//  }
}