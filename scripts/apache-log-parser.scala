import java.io._
import java.util.Scanner
import java.net.URLDecoder

import scala.collection.JavaConversions._
import scala.util.matching.Regex

object ApacheCombinedLogParser extends App {
	val LogLine = """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\s-\s-\s\[([^\]]+)\]\s"GET\s\/sparql\?.*query=([^&]+).*HTTP\/1\.\d"\s200\s(\d+)\s"([^"]*)"\s"([^"]*)".*""".r
	val WhitespaceReplacer = """\s+""".r
	// val URL = """<([^>]*)>""".r

	var totalLines = 0
	var totalDecodingErrors = 0
	var summary = "\n\n"

	args.foreach( filename => {
		var lines = 0
		var decodingErrors = 0

		val scanner = new Scanner( new FileInputStream( filename ), "UTF-8" )

		while( scanner.hasNext ) {
			scanner.nextLine match {
				case LogLine(dateTime, sparql, responseLength, referrer, userAgent) => {
					try {
						val query = WhitespaceReplacer.replaceAllIn( URLDecoder.decode( sparql, "UTF-8" ).trim, " " )

						println( query )
						lines += 1
					}
					catch {
						// java.lang.IllegalArgumentException: URLDecoder: Incomplete trailing escape (%) pattern
						case e: java.lang.IllegalArgumentException => {
							decodingErrors += 1
							System.err.println( "[info] cant' decode: "+sparql )
						}
					}
				}
				case _ => {}
			}
		}

		var infoline = "[result] "+filename+"\t"+lines.toString+" records parsed ("+decodingErrors+" decoding errors)"
		System.err.println( infoline )
		summary += infoline+"\n"

		totalLines += lines
		totalDecodingErrors += decodingErrors
	})

	System.err.println( summary )
	System.err.println( "[result] total\t"+totalLines+" ("+totalDecodingErrors+" decoding errors)" )
}

ApacheCombinedLogParser.main( args )