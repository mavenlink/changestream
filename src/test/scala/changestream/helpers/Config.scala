package changestream.helpers

import com.typesafe.config.ConfigFactory

trait Config {
  val testConfig = ConfigFactory.load("test.conf")
  val awsConfig = testConfig.getConfig("changestream")
  val gcpConfig = testConfig.getConfig("changestream")
}
