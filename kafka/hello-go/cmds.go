package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func ReadConfig(ty string) *kafka.ConfigMap {
	viper.SetConfigName("hello")
	viper.SetConfigType("ini")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("viper: %v", err))
	}

	cm := kafka.ConfigMap{}

	for k, v := range viper.GetStringMapString("default") {
		cm[k] = v
	}
	for k, v := range viper.GetStringMapString(ty) {
		cm[k] = v
	}
	return &cm
}

var (
	topic string = "purchases"
)

func consumer(cm *kafka.ConfigMap) {
	c, err := kafka.NewConsumer(cm)

	if err != nil {
		panic(fmt.Errorf("create consumer: %v", err))
	}

	defer func() {
		tp, err := c.Commit()
		if err != nil {
			fmt.Printf("commit:%v\n", err)
		} else {
			for _, p := range tp {
				fmt.Printf("commit %d:%d\n", p.Partition, p.Offset)
			}
		}
		c.Close()
	}()

	if err := c.Subscribe(topic, nil); err != nil {
		panic(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-sigchan:
			fmt.Printf("signal recevied.\n")
			return

		default:
			// ev := c.Poll(int(time.Millisecond * 100))
			ev := c.Poll(100)
			switch msg := ev.(type) {
			case *kafka.Message:
				if msg.TopicPartition.Error != nil {
					continue
				}
				fmt.Printf("Received topic(%s, part %d): off = %d, key = %v, value = %v\n", *msg.TopicPartition.Topic,
					msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))
				c.CommitMessage(msg)

			case *kafka.AssignedPartitions:
				for _, tp := range msg.Partitions {
					fmt.Printf("assigned: %s:%d\n", *tp.Topic, tp.Partition)
				}

			case *kafka.RevokedPartitions:
				for _, tp := range msg.Partitions {
					fmt.Printf("revoked: %s:%d\n", *tp.Topic, tp.Partition)
				}
			}
		}
	}
}

func producer(cmd *cobra.Command, cm *kafka.ConfigMap) {
	p, err := kafka.NewProducer(cm)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("deliver error: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("deliver event to %s: key = %v, val = %v\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := []string{"eabara", "jsmith", "sgarcia", "jbernard", "awalther"}
	items := []string{"book", "alarm", "t-shirts", "gift card", "batteries"}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	tick := time.Tick(time.Second)
loop:
	for {
		select {
		case <-sigchan:
			fmt.Printf("signal recevied.\n")
			break loop

		case <-tick:
			key := fmt.Sprintf("%s%02d", users[rand.Intn(len(users))], rand.Intn(100))
			val := fmt.Sprintf("%s%02d", items[rand.Intn(len(items))], rand.Intn(100))

			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(key),
				Value:          []byte(val),
			}, nil)
		}
	}

	p.Flush(1000)
}

func admin(cmd *cobra.Command, cm *kafka.ConfigMap) {
	a, err := kafka.NewAdminClient(cm)
	if err != nil {
		panic(err)
	}
	defer a.Close()

	if ok, err := cmd.Flags().GetBool("list"); ok && err == nil {
		meta, err := a.GetMetadata(nil, true, 1000)
		if err != nil {
			panic(err)
		}

		fmt.Println("Brokers:")
		for _, b := range meta.Brokers {
			fmt.Printf("  broker(%d) %s:%d\n", b.ID, b.Host, b.Port)
		}

		fmt.Println("Topics:")
		for _, t := range meta.Topics {
			fmt.Printf("  topic(%s)\n", t.Topic)
			for _, p := range t.Partitions {
				var bd strings.Builder
				for _, r := range p.Replicas {
					bd.WriteString(fmt.Sprintf("%d ", r))
				}
				replicas := bd.String()
				bd.Reset()
				for _, isr := range p.Isrs {
					bd.WriteString(fmt.Sprintf("%d ", isr))
				}
				isrs := bd.String()

				fmt.Printf("    partition(%d): Leader %d, replicas: %s, isrs: %s\n", p.ID, p.Leader, replicas, isrs)
			}
		}
	}

	if ok, err := cmd.Flags().GetBool("create"); ok && err == nil {
		new_topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
		defer cancel()

		spec := kafka.TopicSpecification{
			Topic:             new_topic,
			NumPartitions:     4,
			ReplicationFactor: 2,
		}
		res, err := a.CreateTopics(ctx, []kafka.TopicSpecification{spec})
		if err != nil {
			return
		}

		for _, tr := range res {
			if tr.Error.Code() != kafka.ErrNoError {
				fmt.Printf("create %s failed: %v\n", tr.Topic, tr.Error)
			}
		}
	}
}

var rootCmd = &cobra.Command{
	Use: "kcli",
}

var produceCmd = &cobra.Command{
	Use:     "produce",
	Aliases: []string{"p"},
	Short:   "produce demo messages",
	Run: func(cmd *cobra.Command, _ []string) {
		cm := ReadConfig("producer")
		producer(cmd, cm)
	},
}

var consumeCmd = &cobra.Command{
	Use:     "consume",
	Aliases: []string{"c"},
	Short:   "consume demo messages",
	Run: func(_ *cobra.Command, _ []string) {
		cm := ReadConfig("consumer")
		consumer(cm)
	},
}

var adminCmd = &cobra.Command{
	Use:     "admin",
	Short:   "admin",
	Aliases: []string{"a"},
	Run: func(cmd *cobra.Command, _ []string) {
		cm := ReadConfig("admin")
		admin(cmd, cm)
	},
}

func init() {
	cobra.OnInitialize(initConfig)
	produceCmd.PersistentFlags().StringP("topic", "t", "", "topic")
	consumeCmd.PersistentFlags().StringP("topic", "t", "", "topic")

	adminCmd.PersistentFlags().StringP("topic", "t", "", "topic")
	adminCmd.PersistentFlags().BoolP("create", "C", false, "create topic")
	adminCmd.PersistentFlags().BoolP("list", "L", false, "list metadata")
	adminCmd.PersistentFlags().BoolP("ignore-meta", "i", false, "ignore offset topic when list metadata")

	rootCmd.AddCommand(produceCmd, consumeCmd, adminCmd)
}

func initConfig() {
}
