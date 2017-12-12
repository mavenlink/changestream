package changestream.actors

import java.nio.charset.Charset
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import spray.json._
import DefaultJsonProtocol._

import com.newrelic.api.agent.Trace

object EncryptorActor {
  case class Plaintext(message: JsObject)
  case class Ciphertext(message: JsObject)
}

class EncryptorActor (
                        config: Config = ConfigFactory.load().getConfig("changestream.encryptor")
                      ) extends Actor {
  import EncryptorActor._

  protected val log = LoggerFactory.getLogger(getClass)

  private val charset = Charset.forName("UTF-8")
  private val decoder = Base64.getDecoder
  private val encoder = Base64.getEncoder

  private val cipher = config.getString("cipher")
  private val decodedKey = decoder.decode(config.getString("key"))
  private val originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, cipher)
  private val encryptEngine = Cipher.getInstance(cipher)
  private val decryptEngine = Cipher.getInstance(cipher)

  private val encryptFields = config.getString("encrypt-fields").toLowerCase().split(',').map(_.trim)

  override def preStart() = {
    encryptEngine.init(Cipher.ENCRYPT_MODE, originalKey)
    decryptEngine.init(Cipher.DECRYPT_MODE, originalKey)
  }

  @Trace (dispatcher=true)
  def receive = {
    case Plaintext(message) =>
      val result = encryptFields(message)
      sender() ! result

    case Ciphertext(message) =>
      val result = decryptFields(message)
      sender() ! result
  }

  @Trace
  private def encryptFields(message: JsObject, jsonPath: String = ""): JsObject = {
    JsObject(
      message.fields.map({
        case (k:String, plaintextValue:JsValue) if encryptFields.contains(getNextJsonPath(jsonPath, k)) =>
          val plaintextBytes = plaintextValue.compactPrint.getBytes(charset)
          val cipherText = encryptEngine.doFinal(plaintextBytes)
          val v = encoder.encodeToString(cipherText)
          k -> JsString(v)
        case (k:String, jsObj: JsObject) =>
          k -> encryptFields(jsObj, getNextJsonPath(jsonPath, k))
        case (k:String, v: JsValue) =>
          k -> v
      })
    )
  }

  @Trace
  private def decryptFields(message: JsObject, jsonPath: String = ""): JsObject = {
    JsObject(
      message.fields.map({
        case (k:String, JsString(ciphertextValue)) if encryptFields.contains(getNextJsonPath(jsonPath, k)) =>
          val ciphertextBytes = decoder.decode(ciphertextValue)
          val plaintextBytes = decryptEngine.doFinal(ciphertextBytes)
          val v = new String(plaintextBytes, charset).parseJson
          k -> v
        case (k:String, jsObj: JsObject) =>
          k -> decryptFields(jsObj, getNextJsonPath(jsonPath, k))
        case (k:String, v: JsValue) =>
          k -> v
      })
    )
  }

  private def getNextJsonPath(jsonPath: String, nextPath: String): String = {
    Seq(jsonPath, nextPath).filter(_.nonEmpty).mkString(".")
  }
}
