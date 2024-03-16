# Spark Development Environment

This module will be used during development
phase to simulate different behaviours
of Spark Structured Streaming in case if needed, 
so we can model same behaviour in Streame.
(Ex: How Structured Streaming handles
edge cases on windowing with different triggers
and watermarks)

## Local Run
To easily setup Spark locally, we are using [Devenv](https://devenv.sh/).
Also to simulate a streaming environment with
the ability of easily giving any kind of input,
we create a TCP connection using a Python script
and use `readStream.format("socket")`
in Spark to read from that socket. 

To enable the interactive environment, first run
the `socket_input.py` script:
```sh
python socket_input.py
```
So that it creates a TCP socket on `localhost:9999`.

Then easily enable Devenv and run your Spark job:
```sh
devenv shell
python sample_spark.py
```

And now on the interactive window which is listening
for input, you can give sample data like this:
```txt
2024-03-16T12:00:01,100
2024-03-16T12:00:45,200
2024-03-16T12:01:10,50
2024-03-16T12:01:30,150
```