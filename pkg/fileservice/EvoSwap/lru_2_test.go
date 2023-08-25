package EvoSwap

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func TestLRU2(t *testing.T) {
	cache := NewCache[int, int](&Config[int, int]{
		Lru2Conf: &LRU2Config[int, int]{
			Capacity: 100 * 4,
		},
	})

	var data []int
	for i := 0; i < 1000; i++ {
		data = append(data, rand.Int()%100)
	}

	for i := 0; i < 1000; i++ {
		cache.Set(context.TODO(), data[i], rand.Int()%1000, 4)
	}

	for i := 0; i < 1000; i++ {
		val, resp := cache.Get(context.TODO(), data[i])
		fmt.Printf("val = %d, resp = %v\n", val, resp)
	}

}
