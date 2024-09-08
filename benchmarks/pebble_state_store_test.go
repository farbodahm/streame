package benchmarks

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/utils"
)

var warehouse_path = "./test"

func write(number_of_repeats int, ss *state_store.PebbleStateStore) {

	for i := 0; i < number_of_repeats; i++ {
		record := utils.NewHeavyRecord(100)
		err := ss.Set(record.Key, record)
		if err != nil {
			panic(err)
		}
	}

}

func read_with_cache(number_of_repeats int, key string, ss *state_store.PebbleStateStore) {
	for i := 0; i < number_of_repeats; i++ {
		_, err := ss.Get(key)
		if err != nil {
			panic(err)
		}
	}

}

func BenchmarkPebbleStateStore_Set(b *testing.B) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		write(500, ss)
	}

	err = ss.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkPebbleStateStore_GetWithCache(b *testing.B) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}

	record := utils.NewHeavyRecord(100)
	err = ss.Set(record.Key, record)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		read_with_cache(500, record.Key, ss)
	}

	err = ss.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPebbleStateStore_GetWithoutCache(b *testing.B) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}

	number_of_repeats := 500
	generated_keys := make([]string, 0, number_of_repeats)

	for i := 0; i < number_of_repeats; i++ {
		record := utils.NewHeavyRecord(100)
		err := ss.Set(record.Key, record)
		if err != nil {
			b.Fatal(err)
		}
		generated_keys = append(generated_keys, record.Key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range generated_keys {
			_, err := ss.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		}

	}

	err = ss.Close()
	if err != nil {
		b.Fatal(err)
	}
}
