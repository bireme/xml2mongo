package e2m

import com.mongodb.client.result.InsertOneResult
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import org.bson.types.ObjectId
import org.bson.{BsonObjectId, BsonString, BsonValue, Document}

import java.util
import scala.util.{Failure, Success}
import scala.util.Try
import scala.jdk.CollectionConverters.*

class MongoExport(database: String,
                  collection: String,
                  clear: Boolean = false,
                  host: Option[String] = None,
                  port: Option[Int] = None,
                  user: Option[String] = None,
                  password: Option[String] = None) {
  require ((user.isEmpty && password.isEmpty) || (user.nonEmpty && password.nonEmpty))

  private val hostStr: String = host.getOrElse("localhost")
  private val portStr: String = port.getOrElse(27017).toString
  private val usrPswStr: String = user match
    case Some(usr) => s"$usr:${password.get}@"
    case None => ""

  // mongodb://[username:password@]host[:port][,...hostN[:port]][/[database][?parameter_list]]
  private val mongoUri: String = s"mongodb://$usrPswStr$hostStr:$portStr"
  private val mongoClient: MongoClient = MongoClients.create(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(database)

  private val coll: MongoCollection[Document] =
    if (clear)
      dbase.getCollection(collection).drop()
      dbase.getCollection(collection)
    else dbase.getCollection(collection)

  def close(): Try[Unit] = Try(mongoClient.close())

  def insertDocument(doc: String): Try[String] = {
    Try {
      val document: Document = Document.parse(doc)
      //coll.insertOne(document).getInsertedId.asString().toString
      coll.insertOne(document).getInsertedId.asObjectId().toString
    }
  }

  def insertDocuments(docs: Seq[String]): Try[Seq[String]] = {
    Try {
      val documents: util.List[Document] = docs.map(Document.parse).asJava
      val insertedIds: util.Collection[BsonValue] = coll.insertMany(documents).getInsertedIds.values()

      insertedIds.asScala.toSeq.map(_.asObjectId().toString)
    }
  }
}
