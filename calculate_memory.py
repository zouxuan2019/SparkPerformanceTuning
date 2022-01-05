def calculate_executor(cluster_info):
    recommend_config = {"spark.serializer": "org.apache.spark.serializer.KryoSerializer",

                        }
    print(recommend_config)
