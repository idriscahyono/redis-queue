package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"time"
)

func main() {
	client := newRedisClient()
	defer client.Close()

	for i := 0; i <= 20; i++ {
		mQueue := fmt.Sprintf("m-queue %v", i)
		err := pushToQueue(client, "q-name", mQueue)
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		message, err := popFromQueue(client, "q-name")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Received message:", message)
	}
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func pushToQueue(client *redis.Client, queueName, message string) error {
	err := client.LPush(queueName, message).Err()
	if err != nil {
		return err
	}
	return nil
}

func popFromQueue(client *redis.Client, queueName string) (string, error) {
	result, err := client.BRPop(0*time.Second, queueName).Result()
	if err != nil {
		return "", err
	}
	if len(result) != 2 {
		return "", fmt.Errorf("unexpected result length")
	}
	return result[1], nil
}
