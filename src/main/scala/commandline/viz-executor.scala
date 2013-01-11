package motifs.commandline

import motifs._

import java.io.PrintWriter
import scala.collection.Iterator
import scala.collection.JavaConversions.asScalaSet

object VizExecutor {
	val graphvizConfig = """
	margin=0.05

	charset="utf-8"
	fontname="Calibri"
	fontsize=11

	splines=true
	labelfloat=true

	overlap=false

	node[fontname="Calibri Bold", fontsize=11, color=cyan4, fillcolor=white, style=filled]
	edge[fontname="Calibri", fontsize=11, len=2]""".stripMargin

	def main( args: Array[String] ): Unit = {
		val dataset = args(0)
		val filename = args(1)

		println( "[info] visualizing queries in "+filename )

		val prefixes = motifs.Prefixes.forDataset( dataset ).get
		val reader = new QueryReader( filename, prefixes )

		val iterator = Iterator.continually( reader.nextQueryGraph ).takeWhile( _ != None ).withFilter( !_.get.isInstanceOf[InvalidGraph] )
		// println( iterator.length )
		for( maybeGraph <- iterator ) {
			val graph = maybeGraph.get match {
				case CompleteGraph( g, _, _, _ ) => {
					println( "complete graph")
					g
				}
				case SubstitutionsNeedingGraph( g, _, _, _, _, _ ) => {
					println("subs graph")
					g
				}
				// case InvalidGraph() => println( "[error] InvalidGraph should not be here" )
			}

			val graphName = filename+"-"+reader.queryCount
			val dotSrc = new PrintWriter( graphName )
			println( "[info] "+graphName )

			dotSrc.println( "digraph G {" )
			dotSrc.println( graphvizConfig )
			for( e <- graph.edgeSet ) {
				dotSrc.println( "\t\""+graph.getEdgeSource( e ).toString+"\" -> \""+graph.getEdgeTarget( e )+"\" [label=\""+e.p.toString+"\"]"  )
			}
			dotSrc.println( "}" )

			dotSrc.close
		}
	}
}
