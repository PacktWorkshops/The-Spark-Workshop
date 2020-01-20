from utilities01_py.helper_python import create_session



session = create_session(2, "Default crawl parsing")

rdd = session.sparkContext.emptyRDD()



rdd.saveAsTextFile()

