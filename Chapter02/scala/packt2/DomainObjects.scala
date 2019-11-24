package packt2

import java.time.Instant
import java.time.format.DateTimeParseException
import scala.collection.mutable

/**
 * Domain objects for the WARC data
 *
 * @author Phil, https://github.com/g1thubhub
 */
case class WetRecord(warcType: String, targetURI: String, date: java.sql.Date, recordID: String, refersTo: String, digest: String, contentType: String, contentLength: Int, plainText: String)

object WetRecord {
  def apply(metaPairs: mutable.Map[String, String], pageContent: String): WetRecord = {
    val warcType = metaPairs.getOrElse("WARC-Type", "")
    val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    val refersTo = metaPairs.getOrElse("WARC-Refers-To", "")
    val digest = metaPairs.getOrElse("WARC-Block-Digest", "")
    val contentType = metaPairs.getOrElse("Content-Type", "")

    var contentLength = -1
    var dateMs = -1L
    try {
      contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
      dateMs = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond
    }
    catch {
      case _: NumberFormatException => System.err.println(s"Malformed contentLength field for record $recordID")
      case _: DateTimeParseException => System.err.println(s"Malformed date field for record $recordID")
      case e: Exception => e.printStackTrace()
    }
    WetRecord(warcType, targetURI, new java.sql.Date(dateMs), recordID, refersTo, digest, contentType, contentLength, pageContent)
  }
}

case class WarcRecord(warcType: String, date: java.sql.Date, recordID: String, contentLength: Int, contentType: String, infoID: String, concurrentTo: String, ip: String, targetURI: String, payloadDigest: String, blockDigest: String, payloadType: String, htmlContentType: String, language: Option[String], htmlLength: Int , htmlSource: String)

object WarcRecord {
  def apply(metaPairs: mutable.Map[String, String], responseMeta: (String, Option[String], Int), sourceHtml: String): WarcRecord = {
    val warcType = metaPairs.getOrElse("WARC-Type", "")
    var dateMs = -1L
    val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    var contentLength = -1
    val contentType = metaPairs.getOrElse("Content-Type", "")
    val infoID = metaPairs.getOrElse("WARC-Warcinfo-ID", "")
    val concurrentTo = metaPairs.getOrElse("WARC-Concurrent-To", "")
    val ip = metaPairs.getOrElse("WARC-IP-Address", "")
    val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    val payloadDigest = metaPairs.getOrElse("WARC-Payload-Digest", "")
    val blockDigest = metaPairs.getOrElse("WARC-Block-Digest", "")
    val payloadType = metaPairs.getOrElse("WARC-Identified-Payload-Type", "")

    try {
      contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
      dateMs = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond
    }
    catch {
      case _: NumberFormatException => System.err.println(s"Malformed contentLength field for record $recordID")
      case _: DateTimeParseException => System.err.println(s"Malformed date field for record $recordID")
      case e: Exception => e.printStackTrace()
    }
    WarcRecord(warcType, new java.sql.Date(dateMs), recordID, contentLength, contentType, infoID, concurrentTo, ip, targetURI, payloadDigest, blockDigest, payloadType, responseMeta._1, responseMeta._2, responseMeta._3, sourceHtml)
  }
}