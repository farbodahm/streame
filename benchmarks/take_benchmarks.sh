#!/bin/bash
# This script will be used as part of CI to run benchmarks

# Get a list of all benchmarks
benchmarks=$(go test ./...  -list Benchmark | grep ^Benchmark)

for benchmark in $benchmarks; do
    echo "Running $benchmark"
    
    cpu_profile="${benchmark}_cpu.prof"
    mem_profile="${benchmark}_mem.prof"

    # Run the benchmark with CPU profiling enabled
    go test -run=^$ -bench "^$benchmark$" github.com/farbodahm/streame/benchmarks -count=10 -cpuprofile="$cpu_profile" -memprofile="$mem_profile"

    echo -e "$benchmark CPU Result:\n"
    go tool pprof -text "$cpu_profile"
    echo -e "$benchmark MEM Result:\n"
    go tool pprof -text "$mem_profile"

    go tool pprof -svg "$cpu_profile" > "$cpu_profile.svg"
    go tool pprof -svg "$mem_profile" > "$mem_profile.svg"
done
