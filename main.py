from calculate_executor import calculate_executor
from yarn_executor import calculate_yarn

# cluster_info = {'no_of_nodes': 40,
#                 'no_cores_per_node': 8,
#                 'memory_per_node_gb': 30,
#                 'largest_shuffle_stage_size_gb': 1000
#                 }
#
# calculate_executor(cluster_info)

yarn_info = {
    'total_avail_memory': 760,
    'total_avail_core': 101,
    'max_memory_per_executor': 80,
    'max_core_per_executor': 13,
    'largest_shuffle_stage_size_gb':300
}



calculate_yarn(yarn_info)

print('a')