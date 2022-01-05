import math



def calculate_yarn(yarn_info):
    final_no_executors, no_cores_per_executor, total_available_cores = calculate_executor_number(yarn_info)
    memory_per_executor = yarn_info['max_memory_per_executor']
    print('memory_per_executor:' + str(memory_per_executor))
    actual_memory_per_executor = memory_per_executor * 0.93  # 7% memory heap overhead or 85%
    no_executors_per_node = total_available_cores / 5
    recommend_config = {"spark.executor.memory": math.floor(actual_memory_per_executor),
                        "spark.executor.instances": mat
                        "spark.executor.cores": math.ceil(no_cores_per_executor),
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.shuffle.service.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": get_min_executors(yarn_info),
                        "spark.dynamicAllocation.maxExecutors": get_max_executors(yarn_info),
                        "spark.dynamicAllocation.initialExecutors": final_no_executors,
                        "spark.sql.shuffle.partitions_1": 3 * total_available_cores,
                        "spark.sql.shuffle.partitions_2": calculate_shuffle_partition(yarn_info)
                        }
    print(recommend_config)


def calculate_shuffle_partition(cluster_info):
    target_size_mb = 200
    largest_shuffle_stage_size_gb = cluster_info['largest_shuffle_stage_size_gb']
    partitions = largest_shuffle_stage_size_gb * 1024 / target_size_mb
    return partitions


def calculate_executor_number(yarn_info, no_cores_per_executor= 5):  # 5 For good HDFS throughput
    total_available_cores = yarn_info['total_avail_core']  # Leave 1 core per node for Hadoop/yarn/os
    no_available_executors = total_available_cores / no_cores_per_executor
    final_no_executors = no_available_executors - 1  # Leave 1 executor for yarn Application Manager
    return final_no_executors, no_cores_per_executor, total_available_cores


def get_max_executors(cluster_info):
    final_no_executors, _, _ = calculate_executor_number(cluster_info, 4)
    return math.ceil(final_no_executors)


def get_min_executors(cluster_info):
    final_no_executors, _, _ = calculate_executor_number(cluster_info, 8)
    return math.floor(final_no_executors)
