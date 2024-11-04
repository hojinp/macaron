import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--expname', type=str, required=True)
parser.add_argument('-m', '--meringue', type=str, required=True)
args = parser.parse_args()

assert args.expname in ['ibm009', 'ibm055', 'ibm058']

target_latencies = {
    'ibm009': 160550, 
    'ibm055': 99428, 
    'ibm058': 139064
}

trace_to_unit_mbs = {
    'ibm009': 1377,
    'ibm055': 2892,
    'ibm058': 3601,
}

unit_mb = trace_to_unit_mbs[args.expname]
total_size = unit_mb * 200
assert unit_mb < 26 * 1024
mini_cache_count = int(total_size / (26 * 1024))

meringue = args.meringue + ":50052"

cmd = f"mbakery --expname {args.expname} --is_on_prem false --warmup_time_min 30 --optimization_interval_sec 900 " + \
    f"--cost_minisim_cache_size_unit_mb {unit_mb} --cost_minisim_cache_count 200 " + \
    f"--meringue_address {meringue} " + \
    f"--local_region us-east-1 --remote_region us-west-1 --object_packing_enabled true --dram_cache_enabled true " + \
    f"--dram_vm_type r5.xlarge --latency_minisim_cache_size_unit_mb 26624 --latency_minisim_cache_count {mini_cache_count} --latency_target {target_latencies[args.expname]}"
print(cmd)
os.system(cmd)
