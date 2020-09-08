from pyspark import StorageLevel

gpa.persist(StorageLevel.MEMORY_ONLY)
gpa.cache()

gpa.unpersist()