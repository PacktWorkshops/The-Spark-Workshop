import org.apache.spark.storage.StorageLevel

gpa.persist(StorageLevel.MEMORY_ONLY)
gpa.cache()

gpa.unpersist()