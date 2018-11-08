package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/demizer/go-logs/src/logs"
	"github.com/hashicorp/consul/api"
	"github.com/urfave/cli"
)

func consulConnect(consulAddress string) *api.Client {
	logs.Printf("Acquiring data from %v\n", consulAddress)
	consulConfig := &api.Config{
		Address: consulAddress,
		Scheme:  "http",
	}
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return nil
	}
	return client
}

func backupFromConsul(consulAddress string) error {
	if client := consulConnect(consulAddress); client != nil {
		KV := client.KV()

		if kvpairs, _, err := KV.List("", nil); err == nil {
			logs.Printf("KV Pairs found: %d\n", len(kvpairs))
			if len(kvpairs) != 0 {

				if f, err := os.Create("backup.consul"); err == nil {
					defer f.Close()
					for _, pair := range kvpairs {
						dataDump := fmt.Sprintf("%v %s;", pair.Key, pair.Value)
						f.Write([]byte(dataDump))
						f.Sync()
						logs.Printf("KV: %v %s\n", pair.Key, pair.Value)
					}
				} else {
					logs.Criticalln(err)
					return err
				}
			}
		} else {
			return err
		}
	}

	return nil
}

func restoreToConsul(consulAddress string) error {

	//checking backup file
	if backupData, err := ioutil.ReadFile("backup.consul"); err == nil {
		if len(backupData) != 0 {
			logs.Println("Connecting to the Consul cluster")
			if client := consulConnect(consulAddress); client != nil {
				splittedKV := strings.Split(string(backupData), ";")
				logs.Printf("Gathered %d values from backup\n", len(splittedKV))
				for _, pair := range splittedKV {
					if pair != "" {
						key := strings.Split(pair, " ")[0]
						value := strings.Split(pair, " ")[1]
						p := &api.KVPair{Key: key, Value: []byte(value)}
						_, err = client.KV().Put(p, nil)
						if err != nil {
							continue
							logs.Criticalf("Unable to restore value for key: %v", key)
						}
						logs.Printf("Restored: %v\n", pair)
					}
				}
			}
		}

	} else {
		logs.Criticalln("Error while opening backup file")
		return err
	}

	return nil
}

func main() {

	app := cli.NewApp()
	app.Name = "consulKVhelper"
	app.Usage = "make an explosive entrance"
	app.Version = "1.0beta"
	app.Action = func(c *cli.Context) error {
		logs.Println("Please run with -help if needed")
		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:  "backup",
			Usage: "backup kv values from consul server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "address,a",
					Usage: "Consul Server Address",
				},
			},
			Action: func(ctx *cli.Context) error {
				if ctx.String("address") == "" {
					return cli.NewExitError("consul address should be specified", 1)
				}
				consulAddress := ctx.String("address")
				if err := backupFromConsul(consulAddress); err != nil {
					logs.Criticalln(err.Error())
					os.Exit(1)
				}
				logs.Println("Backup successfully completed")
				return nil
			},
		},
		{
			Name:  "restore",
			Usage: "restore kv from file to consul server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "address,a",
					Usage: "Consul Server Address",
				},
			},
			Action: func(ctx *cli.Context) error {
				if ctx.String("address") == "" {
					return cli.NewExitError("consul address should be specified", 1)
				}
				consulAddress := ctx.String("address")

				if err := restoreToConsul(consulAddress); err != nil {
					logs.Criticalln(err.Error())
					os.Exit(1)
				}
				logs.Println("Backup successfully restored")
				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logs.Criticalln(err.Error())
	}
}
