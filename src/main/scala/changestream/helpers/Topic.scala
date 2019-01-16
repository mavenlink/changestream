package changestream.helpers

import changestream.events.MutationEvent

object Topic {
  def getTopic(mutation: MutationEvent, topic: String, topicHasVariable: Boolean = true): String = {
    val database = mutation.database.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    val tableName = mutation.tableName.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    topicHasVariable match {
      case true => topic.replace("{database}", database).replace("{tableName}", tableName)
      case false => topic
    }
  }
}
