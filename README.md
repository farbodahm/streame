# Streame

- [Streame](#streame)
  - [Status Tracker](#status-tracker)
  - [Benchmarks](#benchmarks)
  - [Available Functionalities](#available-functionalities)
  - [Final Workflow Expectations](#final-workflow-expectations)
  - [Contributions](#contributions)

Streame is going to be a fault tolerant stream processor, Eventually!

## Status Tracker
You can track the roadmap and in-progress features of Streame
on [Streame Status Tracker](https://github.com/users/farbodahm/projects/1).

## Benchmarks
Streame emphasizes the importance of benchmarks, as nothing is reliable
without numbers.

Each core functionality will have benchmark tests in `benchmarks` module.
These benchmarks are integrated into our CI process, ensuring automatic
execution.

Additionally, detailed SVG reports on Memory and CPU performance,
generated using pprof, are automatically uploaded to the artifacts
section of each CI pipeline run.

## Available Functionalities

- [x] Schema Validation
- [x] Filter
- [x] Select
- [x] Add static column
- [ ] Join
- [ ] Aggregation

## Final Workflow Expectations

Thinking loudly, here I will write all of the requirements that I
expect to be added to the main framework somewhere in the future
shortly after having the main functionalities in place.

- Orchestration (Ex. K8S, DockerSwarm, …)
- Monitoring
- Unit & Integration tests
- Load Test (Ex. K6S)
- Fault Tolerance Test (Ex. chaos-mesh)

## Contributions

This project is getting started as fun and for learning; and it may
end up in something important to be used on Production; So feel free
to contribute to it as you like ;)
You can find the roadmap [here](https://github.com/users/farbodahm/projects/1).