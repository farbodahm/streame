# Interesting Challenges

- [Interesting Challenges](#interesting-challenges)
  - [Challenges](#challenges)
    - [Ensuring Unique Schemas for Each Stage](#ensuring-unique-schemas-for-each-stage)
      - [Problem](#problem)
      - [Solution](#solution)

While writing a stream processor, you may encounter different challenges.
I will try to document the interesting ones here.

## Challenges

### Ensuring Unique Schemas for Each Stage
#### Problem

Consider an input stream with a schema like this:

`(name, email, age)`

Suppose you add a `filter` stage to only accept records with a specific *age*. Following that, you add a `select` stage to choose only *name* and *email*, resulting in a schema like this:

`(name, email)`

In all stages you define, the first stage will always be a **schema validator**. To recap, schema validator makes sure
that input records are following the given schema.

How can you design your processor to ensure that each stage maintains its unique schema?


#### Solution

When defining a new stage, we create a new executor,
which is essentially a function. This means we are
defining a function within a function.
Does that ring a bell??

Exactly! [Closure!](https://gobyexample.com/closures)

By leveraging the power of closures and how the executor gets
**bound** to its variables, we can ensure that each stage
maintains its unique schema. However, there is a caveat.

Since
schemas are maps and thus referenced, we must ensure that
other stages do not overwrite the schema. If they need to
change the schema, they must deep copy the old schema and
return a new one, ensuring subsequent stages do not override
previous ones.

Have a look at this code to get more context:

```go

func (sdf *StreamDataFrame) Select(columns ...string) DataFrame {
	new_schema, err := functions.ReduceSchema(sdf.Schema, columns...)
	if err != nil {
		panic(err)
	}

	new_sdf := StreamDataFrame{
		SourceStream: sdf.SourceStream,
		OutputStream: sdf.OutputStream,
		ErrorStream:  sdf.ErrorStream,
		Stages:       sdf.Stages,
		Schema:       new_schema,
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		result := functions.ApplySelect(data, columns...)
		return []types.Record{result}, nil
	}

	new_sdf.addToStages(executor)
	return &new_sdf
}

```