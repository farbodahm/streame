# Streame

- [Streame](#streame)
  - [Final Workflow Expectations](#final-workflow-expectations)
  - [Engine functional requirements](#engine-functional-requirements)
  - [Starting Examples](#starting-examples)
  - [Contributions](#contributions)

Streame is going to be a fault tolerant stream processor, Eventually!


## Final Workflow Expectations

Thinking loudly, here I will write all of the requirements that I
expect to be added to the main framework somewhere in the future
shortly after having the main functionalities in place.

- Orchestration (Ex. K8S, DockerSwarm, …)
- Monitoring
- Unit & Integration tests
- Load Test (Ex. K6S)
- Fault Tolerance Test (Ex. chaos-mesh)

## Engine functional requirements

Again, thinking loudly, I will write down the functional requirements
that a stream processor can have to shape up the next steps that *may* be implemented.

- Single node
    - [ ] simple filter
    - [ ] simple join
    - [ ] simple aggregation
    - [ ] windowing
    - [ ] watermarking
    - [ ] trigger
- Distributed (2 or more nodes)
    - [ ]  simple filter
    - [ ]  simple join
    - [ ]  simple aggregation
  
## Starting Examples

- Word counter
- Sum scores for leader board

## Contributions

This project is getting started as fun and for learning; and it may
end up in something important to be used on Production; So feel free
to contribute to it as you like ;)