package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c, mqErr := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{"127.0.0.1:9876"}),
		consumer.WithGroupName("purchase"),
		//consumer.WithConsumerModel(consumer.BroadCasting),
	)

	if mqErr != nil {
		panic("MQ失败:" + mqErr.Error())
	}

	if err := c.Subscribe("purchase_order", consumer.MessageSelector{}, S3); err != nil {
		fmt.Printf("消费topic：purchase_order失败:%s", err.Error())
	}

	_ = c.Start()

	//接收终止信号
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = c.Shutdown()
}

func S3(c context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error){

	fmt.Println(string(messages[0].Body)+"333333")

	return consumer.ConsumeSuccess, nil
}