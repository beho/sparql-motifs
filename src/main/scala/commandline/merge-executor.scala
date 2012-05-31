package motifs.commandline

import motifs.Merger

object MergeExecutor {
	def main( args: Array[String] ) {
		if( args.length != 2 ) {
			println( "source-dir target-dir" )
			sys.exit(1)
		}

		new Merger( args(0), args(1) ).run
	}
}