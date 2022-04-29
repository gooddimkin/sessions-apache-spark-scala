package org.example

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}

case class Search(
    queryType: String = "",
    queryId: String = "",
    datetime: String = "",
    date: Option[LocalDate] = None,
    params: Map[String, String] = Map[String, String](),
    docs: Seq[String] = Seq[String]()
)

case class DocOpen(
    datetime: String = "",
    date: Option[LocalDate] = None,
    queryId: String = "",
    docId: String = ""
)

object Parser {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists() && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  val formatStrings = Seq("dd.MM.yyyy_HH:mm:ss", "EEE,_d_MMM_yyyy_HH:mm:ss_Z")

  def parseDate(dateString: String): Option[LocalDate] = {
    val parser: String => Option[LocalDate] = dateStr =>
      try {
        Some(
          LocalDate
            .parse(
              dateString,
              DateTimeFormatter.ofPattern(dateStr, Locale.US)
            )
        )
      } catch {
        case e: Exception => None
      }
    val dates = formatStrings.map(parser).filter(_.isDefined)
    if (dates.nonEmpty) dates.head
    else None
  }

  def parseFile(f: File): Seq[Any] = {
    val sessionId = f.getName
    val bs: BufferedSource = Source.fromFile(f, enc = "windows-1251")

    @tailrec
    def loopSearch(
        it: Iterator[String],
        acc: Search = Search()
    ): Search = {
      if (!it.hasNext) acc
      else {
        val line = it.next
        line match {
          case l if l.startsWith("CARD_SEARCH_END") => {
            if (it.hasNext) {
              val searchResult = it.next.split(" ")
              Search(
                acc.queryType,
                searchResult.head,
                acc.datetime,
                acc.date,
                acc.params,
                searchResult.tail
              )
            } else acc
          }
          case l => {
            val param = l.split(" ")
            loopSearch(
              it,
              Search(
                acc.queryType,
                acc.queryId,
                acc.datetime,
                acc.date,
                acc.params + (param.head -> param(1)),
                acc.docs
              )
            )
          }
        }
      }
    }

    @tailrec
    def loop(
        it: Iterator[String],
        acc: Seq[Any] = Seq[Any]()
    ): Seq[Any] = {
      if (!it.hasNext) (acc)
      else {
        val line = it.next
        line match {
          case l if l.startsWith("QS") => {
            val params = l.split(" ")
            if (it.hasNext) {
              val searchResult = it.next.split(" ")
              loop(
                it,
                Search(
                  "quick",
                  s"${sessionId}_${searchResult(0)}",
                  params(1),
                  parseDate(params(1)),
                  Map("query" -> params(2)),
                  searchResult.tail
                ) +: acc
              )
            } else
              (
                Search(
                  queryType = "quick",
                  datetime = params(1),
                  date = parseDate(params(1)),
                  params = Map("query" -> params(2))
                ) +: acc
              )
          }
          case l if l.startsWith("CARD_SEARCH_START") => {
            val datetime = l.split(" ")(1)
            loop(
              it,
              loopSearch(
                it,
                Search(
                  queryType = "card",
                  datetime = datetime,
                  date = parseDate(datetime)
                )
              ) +: acc
            )
          }
          case l if l.startsWith("DOC_OPEN") => {
            val info = l.split(" ")
            loop(
              it,
              DocOpen(
                info(1),
                parseDate(info(1)),
                s"${sessionId}_${info(2)}",
                info(3)
              ) +: acc
            )
          }
          case _ => loop(it, acc)
        }

      }
    }

    val it = bs.getLines
    val data = loop(it)

    bs.close()
    data
  }
}
