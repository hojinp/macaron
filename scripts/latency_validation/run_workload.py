import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--expname', type=str, required=True) # e.g., ibm058
parser.add_argument('-t', '--tracepath', type=str, required=True)
parser.add_argument('-m', '--mbakery', type=str, required=True) # ipaddress
args = parser.parse_args()

mbakery = args.mbakery + ":50051"
cmd = f"workload_runner --expname={args.expname} --tracepath={args.tracepath} --mbakery={mbakery} --loglatency=true"
print(cmd)
os.system(cmd)