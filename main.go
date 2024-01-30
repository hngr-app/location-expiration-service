package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	url, ok := os.LookupEnv("REDIS_URL")
	if !ok {
		panic("Missing environment variable REDIS_URL!")
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		panic(err)
	}
	opts.DialTimeout = 30 * time.Second

	rdb := redis.NewClient(opts)

	nowMillis := strconv.FormatInt(time.Now().UnixMilli(), 10)

	fmt.Printf("Current timestamp: %s\n", nowMillis)

	rangeResult := rdb.ZRangeByScore(ctx, "restaurant-location-exp-ts", &redis.ZRangeBy{
		Min:    "-inf",
		Max:    nowMillis,
		Offset: 0,
		Count:  -1,
	})

	restaurantIds, err := rangeResult.Result()
	if err != nil {
		panic(err)
	}

	if len(restaurantIds) == 0 {
		fmt.Println("No restaurant location data has expired!")
		os.Exit(0)
	}

	fmt.Printf("Deleting the following restaurant locations from the cache\n\n%s\n", restaurantIds)

	locationRemResult := rdb.ZRem(ctx, "restaurant-location", restaurantIds)

	locationRemCount, err := locationRemResult.Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Removed %d restaurant locations!\n", locationRemCount)

	exptsRemResult := rdb.ZRem(ctx, "restaurant-location-exp-ts", restaurantIds)

	exptsRemCount, err := exptsRemResult.Result()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Removed %d expiration timestamps!\n", exptsRemCount)
	fmt.Println("Task finished successfully.")
}
