package changestream

import java.util

import collection.JavaConverters._
import changestream.events.{Delete, Insert, Update}
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization._
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream

import com.newrelic.api.agent.Trace

object ChangestreamEventDeserializer extends {
  val tableMapData = new util.HashMap[java.lang.Long, TableMapEventData]
  private val _deserializers = new util.IdentityHashMap[EventType, EventDataDeserializer[_ <: EventData]]
  var lastQuery: Option[String] = None
} with EventDeserializer(new EventHeaderV4Deserializer, new NullEventDataDeserializer, _deserializers, tableMapData) {
  setEventDataDeserializer(EventType.QUERY, new QueryEventDataDeserializer)
  setEventDataDeserializer(EventType.TABLE_MAP, new TableMapEventDataDeserializer)
  setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer)
  setEventDataDeserializer(EventType.XID, new XidEventDataDeserializer)

  setEventDataDeserializer(EventType.ROWS_QUERY, RowsQueryDeserializer)
  setEventDataDeserializer(EventType.WRITE_ROWS, new InsertDeserializer(tableMapData))
  setEventDataDeserializer(EventType.UPDATE_ROWS, new UpdateDeserializer(tableMapData))
  setEventDataDeserializer(EventType.DELETE_ROWS, new DeleteDeserializer(tableMapData))
  setEventDataDeserializer(EventType.EXT_WRITE_ROWS, new InsertDeserializer(tableMapData, true))
  setEventDataDeserializer(EventType.EXT_UPDATE_ROWS, new UpdateDeserializer(tableMapData, true))
  setEventDataDeserializer(EventType.EXT_DELETE_ROWS, new DeleteDeserializer(tableMapData, true))

  setEventDataDeserializer(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer)
  setEventDataDeserializer(EventType.ROTATE, new RotateEventDataDeserializer)
  setEventDataDeserializer(EventType.INTVAR, new IntVarEventDataDeserializer)

  @volatile protected var _nextSequenceNumber: Long = 0
  def getNextSequenceNumber(rowsInMutation: Long = 1): Long = {
    val next = _nextSequenceNumber
    _nextSequenceNumber += rowsInMutation
    next
  }

  def getCurrentSequenceNumber = _nextSequenceNumber
}

class InsertDeserializer(
                          tableMap: util.HashMap[java.lang.Long, TableMapEventData],
                          mayContainExtraInformation: Boolean = false
                        ) extends EventDataDeserializer[Insert] {
  private val internalDeserializer = new WriteRowsEventDataDeserializer(tableMap).
    setMayContainExtraInformation(mayContainExtraInformation)

  @Trace
  override def deserialize(inputStream: ByteArrayInputStream): Insert = {
    val result = internalDeserializer.deserialize(inputStream)
    val rows = result.getRows.asScala.toList

    Insert(
      result.getTableId,
      result.getIncludedColumns,
      rows,
      tableMap.get(result.getTableId).getDatabase,
      tableMap.get(result.getTableId).getTable,
      ChangestreamEventDeserializer.lastQuery,
      ChangestreamEventDeserializer.getNextSequenceNumber(rows.size)
    )
  }
}

class UpdateDeserializer(
                          tableMap: util.HashMap[java.lang.Long, TableMapEventData],
                          mayContainExtraInformation: Boolean = false
                        ) extends EventDataDeserializer[Update] {
  private val internalDeserializer = new UpdateRowsEventDataDeserializer(tableMap).
    setMayContainExtraInformation(mayContainExtraInformation)

  @Trace
  override def deserialize(inputStream: ByteArrayInputStream): Update = {
    val result = internalDeserializer.deserialize(inputStream)
    // pre-processing so that we can implement a consistent interface for "rows" across all mutation types
    val (oldRows, rows) = result.getRows.asScala.toList.unzip(pair => (pair.getKey, pair.getValue))

    Update(
      result.getTableId,
      result.getIncludedColumns,
      result.getIncludedColumnsBeforeUpdate,
      rows,
      oldRows,
      tableMap.get(result.getTableId).getDatabase,
      tableMap.get(result.getTableId).getTable,
      ChangestreamEventDeserializer.lastQuery,
      ChangestreamEventDeserializer.getNextSequenceNumber(rows.size)
    )
  }
}

class DeleteDeserializer(
                          tableMap: util.HashMap[java.lang.Long, TableMapEventData],
                          mayContainExtraInformation: Boolean = false
                        ) extends EventDataDeserializer[Delete] {
  private val internalDeserializer = new DeleteRowsEventDataDeserializer(tableMap).
    setMayContainExtraInformation(mayContainExtraInformation)

  @Trace
  override def deserialize(inputStream: ByteArrayInputStream): Delete = {
    val result = internalDeserializer.deserialize(inputStream)
    val rows = result.getRows.asScala.toList

    Delete(
      result.getTableId,
      result.getIncludedColumns,
      rows,
      tableMap.get(result.getTableId).getDatabase,
      tableMap.get(result.getTableId).getTable,
      ChangestreamEventDeserializer.lastQuery,
      ChangestreamEventDeserializer.getNextSequenceNumber(rows.size)
    )
  }
}

object RowsQueryDeserializer extends EventDataDeserializer[EventData] {
  @Trace
  override def deserialize(inputStream: ByteArrayInputStream) = {
    inputStream.skip(1)
    val len = inputStream.available()
    val query = inputStream.readString(len)
    ChangestreamEventDeserializer.lastQuery = Some(query)

    null //scalastyle:ignore
  }
}
