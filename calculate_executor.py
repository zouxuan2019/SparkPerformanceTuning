import math

cluster_info = {'no_of_nodes': 10,
                'no_cores_per_node': 16,
                'memory_per_node_gb': 64
                }


def calculate_executor():
    final_no_executors, no_cores_per_executor, total_available_cores = calculate_executor_number()
    no_executors_per_node = final_no_executors / cluster_info['no_of_nodes']
    memory_per_executor = cluster_info['memory_per_node_gb'] / no_executors_per_node
    print('memory_per_executor:' + str(memory_per_executor))
    actual_memory_per_executor = memory_per_executor * 0.93  # 7% memory heap overhead
    recommend_config = {"spark.executor.memory": math.floor(actual_memory_per_executor),
                        "spark.executor.instances": math.ceil(no_executors_per_node),
                        "spark.executor.cores": math.ceil(no_cores_per_executor),
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.shuffle.service.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": get_min_executors(),
                        "spark.dynamicAllocation.maxExecutors": get_max_executors(),
                        "spark.dynamicAllocation.initialExecutors": final_no_executors,
                        "spark.sql.shuffle.partitions": 3 * total_available_cores
                        }
    print(recommend_config)


def calculate_executor_number(no_cores_per_executor=5):  # 5 For good HDFS throughput
    total_available_cores = (cluster_info['no_cores_per_node'] - 1) * cluster_info[
        'no_of_nodes']  # Leave 1 core per node for Hadoop/yarn/os
    no_available_executors = total_available_cores / no_cores_per_executor
    final_no_executors = no_available_executors - 1  # Leave 1 executor for yarn Application Manager
    return final_no_executors, no_cores_per_executor, total_available_cores


def get_max_executors():
    final_no_executors, _, _ = calculate_executor_number(4)
    return math.ceil(final_no_executors)


def get_min_executors():
    final_no_executors, _, _ = calculate_executor_number(8)
    return math.floor(final_no_executors)


calculate_executor()
