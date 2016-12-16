package changestream.actors

import java.util

import scala.concurrent.duration._
import scala.language.postfixOps
import spray.json._
import DefaultJsonProtocol._
import changestream.helpers.Fixtures
import akka.actor.{ActorRef, ActorRefFactory, Props}
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.EncryptorActor.Ciphertext
import changestream.events._
import changestream.helpers.{Base, Config}
import com.typesafe.config.ConfigFactory

import scala.util.Random
import scala.collection.immutable.ListMap
import scala.concurrent.Await

class JsonFormatterActorSpec extends Base with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val formatterActor = TestActorRef(Props(classOf[JsonFormatterActor], maker, testConfig.getConfig("changestream")))

  val database = "changestream_test"
  val tableName = "users"

  private def jsStringPrinter(v: JsValue) = v match {
    case JsString(s) => s
    case x => s"invalid string: ${x}"
  }
  private def tryJsNumber(v: JsValue) = v match {
    case JsNumber(n) => n
    case x => s"invalid number: ${x}"
  }

  private def getJsFields(v: JsValue): Map[String, JsValue] = v match {
    case obj: JsObject => obj.fields
    case _ => Map[String, JsValue]()
  }

  def expectJsonFrom(
                      mutation: MutationWithInfo,
                      eventCount: Int = 0,
                      actor: ActorRef = formatterActor
                    ): Seq[Map[String, JsValue]] = {
    actor ! mutation

    val messages = probe.receiveN(eventCount, 5000 milliseconds)
    messages.map({
      case MutationWithInfo(m, tx, col, Some(message: String), pos) =>
        m should be(mutation.mutation)
        tx should be(mutation.transaction)
        col should be(mutation.columns)
        pos should be(mutation.position)

        getJsFields(message.parseJson)
    })
  }

  def expectJsonFrom2(
                      mutation: MutationWithInfo,
                      eventCount: Int = 0,
                      actor: ActorRef = formatterActor
                    ): Seq[JsObject] = {
    actor ! mutation

    val messages = probe.receiveN(eventCount, 5000 milliseconds)
    messages.map({
      case MutationWithInfo(m, tx, col, Some(message: String), pos) =>
        m should be(mutation.mutation)
        tx should be(mutation.transaction)
        col should be(mutation.columns)
        pos should be(mutation.position)

        message.parseJson.asJsObject
    })
  }

  def beValidInsert(json: Map[String, JsValue]) = {
    "be an insert mutation" in {
      json should contain key ("mutation")
      json("mutation").toString(jsStringPrinter) should be("insert")
    }
  }
  def beValidUpdate(json: Map[String, JsValue], oldData: Map[String, Any]) = {
    "be an update mutation" in {
      json should contain key ("mutation")
      json("mutation").toString(jsStringPrinter) should be("update")
    }

    json should contain key ("old_row_data")
    val oldJsonData = getJsFields(json("old_row_data"))
    haveValidData(oldJsonData, oldData, isOld = true)
  }
  def beValidDelete(json: Map[String, JsValue]) = {
    "be a delete mutation" in {
      json should contain key ("mutation")
      json("mutation").toString(jsStringPrinter) should be("delete")
    }
  }

  def haveValidTransactionInfo(json: Map[String, JsValue]) = {
    "have valid transaction id" in {
      json should contain key "transaction"
      val txJson = getJsFields(json("transaction"))
      txJson should contain key "id"
      txJson("id").toString(jsStringPrinter) should fullyMatch regex "(?i)([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}(:\\d+)?"
    }

    "have valid transaction row_count" in {
      json should contain key "transaction"
      val txJson = getJsFields(json("transaction"))
      val rowCountCheck = txJson("row_count") match {
        case JsNumber(num) if (num >= 1) =>
          "valid number"
        case _ =>
          "invalid"
      }
      rowCountCheck should be("valid number")
    }
  }

  def haveNoTransactionInfo(json: Map[String, JsValue]) = {
    "have no transaction info" in {
      json shouldNot contain key "transaction"
    }
  }

  def haveValidMetadata(json: Map[String, JsValue], database: String, tableName: String, sql: String, timestamp: Long) = {
    "has valid database" in {
      json should contain key "database"
      json("database").toString(jsStringPrinter) should be(database)
    }
    "has valid table" in {
      json should contain key "table"
      json("table").toString(jsStringPrinter) should be(tableName)
    }
    "has valid sql" in {
      json should contain key "query"
      val query = getJsFields(json("query"))
      query should contain key "sql"
      query("sql").toString(jsStringPrinter) should be(sql)
    }
    "has valid timestamp" in {
      json should contain key "query"
      val query = getJsFields(json("query"))
      query should contain key "timestamp"
      tryJsNumber(query("timestamp")) should be(timestamp)
    }
  }

  def haveValidPrimaryKey(json: Map[String, JsValue], primaryKey: Map[String, Any]) = {
    "has a valid primary key" in {
      primaryKey.size should be > 0
      json should contain key "primary_key"
      val pk = getJsFields(json("primary_key"))

      primaryKey.foreach({
        case (f, v) =>
          pk should contain key f
          getRawJsonValue(pk(f)) should be(v)
      })
    }
  }

  def haveValidData(jsonData: Map[String, JsValue], data: Map[String, Any], isOld: Boolean = false) = {
    (isOld match { case true => "have valid old data" case false => "have valid data" }) should {
      data.foreach({
        case (k, v) =>
          s"field ${k}" in {
            val dataVal = getRawDataValue(v)
            if(dataVal == null) { //scalastyle:ignore
              jsonData shouldNot contain key k
            }
            else {
              val jsonVal = getRawJsonValue(jsonData(k))
              jsonData should contain key k
              jsonVal should be(dataVal)
            }
          }
      })
    }
  }

  def getRawDataValue(v: Any) = {
    v match {
      case arr: Array[Byte] =>
        null //scalastyle:ignore
      case bs: util.BitSet if bs.size == 1 => bs.get(0)
      case bs: util.BitSet => (0 to bs.size - 1).map(bs.get(_))
      case timestamp: java.sql.Timestamp => timestamp.getTime
      case time: java.sql.Time => time.toString
      case date: java.sql.Date => date.toString
      case d: java.util.Date => JsonFormatterActor.dateFormatter.format(d)
      //Because spray-json brings all numbers back in as bigdecimal
      case n: Double => BigDecimal(n)
      case n: Float => BigDecimal(n.toDouble)
      case n: Long => BigDecimal(n)
      case n: Int => BigDecimal(n)
      case n: java.math.BigDecimal => BigDecimal(n)
      case _ => v
    }
  }

  def getRawJsonValue(v: JsValue) = {
    v match {
      case JsString(s) => s
      case JsNumber(n) => n
      case JsTrue => true
      case JsFalse => false
      case JsNull => null // scalastyle:ignore
      // array will always be bit
      case JsArray(a) => a.toList.map({ case JsTrue => true case _ => false })
      case o: JsObject => o.fields
    }
  }

  def jsonChecksOut(inTransaction: Boolean, json: Map[String, JsValue], mutation: MutationWithInfo, rowData: ListMap[String, Any]) = {
    val sql = mutation.mutation.sql.get
    val columns = mutation.columns.get
    val timestamp = mutation.mutation.timestamp

    if(inTransaction) {
      haveValidTransactionInfo(json)
    }
    else {
      haveNoTransactionInfo(json)
    }
    val pkFields = columns.columns.collect({
      case Column(name, _, true) => name
    })
    haveValidPrimaryKey(json, rowData.filterKeys(pkFields.contains(_)))
    haveValidMetadata(json, database, tableName, sql, timestamp)
    val jsonData = getJsFields(json("row_data"))
    haveValidData(jsonData, rowData)
  }

  def crudAllChecksOut(inTransaction: Boolean, rowCount: Int, transactionRowCount: Int) = {
    Seq("insert", "update", "delete").foreach(mutationType =>
      s"when mutation is a/an ${mutationType}" should {
        val (mutation, rowData, oldRowData) = Fixtures.mutationWithInfo(mutationType, rowCount, transactionRowCount, inTransaction)
        val jsons = expectJsonFrom(mutation, rowCount)

        jsons.zipWithIndex.foreach({
          case (json, idx) =>
            s"have valid message for row ${idx}" should {
              if (mutationType == "insert") {
                beValidInsert(json)
              }
              else if (mutationType == "update") {
                beValidUpdate(json, oldRowData(idx))
              }
              else {
                beValidDelete(json)
              }

              jsonChecksOut(inTransaction, json, mutation, rowData(idx))
            }
        })
      }
    )
  }

  "When JsonFormatterActor receives a message with TransactionInfo" should {
    "for single row updates" should {
      crudAllChecksOut(inTransaction = true, 1, 1)
    }
    "for multi-row updates" should {
      val rowCount = 10
      val transactionCount = rowCount + Random.nextInt(100)
      crudAllChecksOut(inTransaction = true, rowCount, transactionCount)
    }
  }

  "When JsonFormatterActor receives a message without TransactionInfo" should {
    "for single row updates" should {
      crudAllChecksOut(inTransaction = false, 1, 1)
    }
    "for multi-row updates" should {
      val rowCount = 10
      val transactionCount = rowCount + Random.nextInt(100)
      crudAllChecksOut(inTransaction = false, rowCount, transactionCount)
    }
  }

  "When JsonFormatterActor receives many messages" should {
    "row_count stats should be correct" in {
      val (mutation, rowData, oldRowData) = Fixtures.mutationWithInfo("update", 100, 100, true)
      val jsons = expectJsonFrom(mutation, 100)

      jsons.zipWithIndex.foreach({
        case (json, idx) =>
          json should contain key "query"
          val query = getJsFields(json("query"))
          tryJsNumber(query("row_count")) should be(100)
          tryJsNumber(query("current_row")) should be(idx + 1)
      })
    }
  }

  "When JsonFormatterActor receives changes with many rows" should {
    "sequence number should be correct" in {
      val (mutation, rowData, oldRowData) = Fixtures.mutationWithInfo("update", 10, 10, sequenceNext = 0)
      val jsons = expectJsonFrom(mutation, 10)

      val (mutation2, rowData2, oldRowData2) = Fixtures.mutationWithInfo("update", 10, 10, sequenceNext = 10)
      val jsons2 = expectJsonFrom(mutation2, 10)

      jsons.zipWithIndex.foreach({
        case (json, idx) =>
          json should contain key "sequence"
          tryJsNumber(json("sequence")) should be(idx)
      })

      jsons2.zipWithIndex.foreach({
        case (json, idx) =>
          json should contain key "sequence"
          tryJsNumber(json("sequence")) should be(idx + 10)
      })
    }
  }

  "When JsonFormatterActor receives a message with missing info" should {
    "refuse to process the message" in {
      val (validMutation, _, _) = Fixtures.mutationWithInfo("insert")
      formatterActor ! validMutation
      probe.receiveN(1)
      formatterActor ! validMutation.copy(columns = None)
      probe.expectNoMsg()
    }
  }

  "When JsonFormatterActor receives a message with include-data disabled" should {
    "not include data" in {
      val configNoData = ConfigFactory
        .parseString("changestream.include-data = false")
        .withFallback(testConfig)
        .getConfig("changestream")
      val actorNoData = TestActorRef(Props(classOf[JsonFormatterActor], maker, configNoData))

      val (validMutation, _, _) = Fixtures.mutationWithInfo("update")
      val json = expectJsonFrom(validMutation, 1, actorNoData).head
      json shouldNot contain key "row_data"
      json shouldNot contain key "old_row_data"
    }
  }

  "When JsonFormatterActor receives a message with encrypt-data enabled" should {
    "encrypt data" should {
      val configEncrypt = ConfigFactory
        .parseString("changestream.encryptor.enabled = true")
        .withFallback(testConfig)
        .getConfig("changestream")
      val formatAndDecryptActor = TestActorRef(Props(classOf[JsonFormatterActor], maker, configEncrypt))
      val decryptActor = TestActorRef(Props(classOf[EncryptorActor], configEncrypt.getConfig("encryptor")))

      val (validMutation, data, oldData) = Fixtures.mutationWithInfo("update")
      val json = expectJsonFrom2(validMutation, 1, formatAndDecryptActor).head

      json.fields("row_data").isInstanceOf[JsString] should be(true)
      json.fields("old_row_data").isInstanceOf[JsString] should be(true)

      val decryptJson = Await.result(decryptActor ? Ciphertext(json), 3000 milliseconds) match {
        case json: JsValue => json
      }

      haveValidData(getJsFields(decryptJson.asJsObject.fields("row_data")), data.head, false)
      haveValidData(getJsFields(decryptJson.asJsObject.fields("old_row_data")), oldData.head, true)
    }
  }
}
