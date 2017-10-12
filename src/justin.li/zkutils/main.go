package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	zkTIMEOUT = time.Second * 10
)

const (
	eUsage = 1
	eFail  = 2
)

type ZkNode struct {
	Path     string
	Data     []byte
	Children []*ZkNode
}

func retrieveNode(zkconn *zk.Conn, path string) (*ZkNode, error) {
	data, stat, err := zkconn.Get(path)
	if err != nil {
		return nil, err
	}
	nd := &ZkNode{Path: path}
	if stat.NumChildren > 0 {

	}
	fmt.Printf("%+v %+v\n", data, stat)
	return nd, nil
}

func dump(zkconn *zk.Conn, rootPath, filePath string) error {
	zknd, err := retrieveNode(zkconn, rootPath)
	if err != nil {
		if data, err := json.Marshal(zknd); err == nil {
			return ioutil.WriteFile(filePath, data, os.ModePerm)
		} else {
			return err
		}
	}
	return err
}

func load(zkconn *zk.Conn, rootPath, filePath string) error {
	return nil
}

type Command struct {
	Name string
	Desc string
	Func func(*zk.Conn, string, string) error
}

var commands = map[string]*Command{
	"dump": &Command{"dump", "dump a zk dir to a file", dump},
	"load": &Command{"load", "load a file to a zk path", load},
}

func main() {
	zkaddr := flag.String("zk", "localhost:2181", "Zookeeper to connect to, e.g. localhost:2181(default value)")
	rootPath := flag.String("rootPath", "/", "root path to dump or restore. default is /")
	filePath := flag.String("file", "", "file to read from or write to")
	flag.Parse()

	usage := func() {
		flag.PrintDefaults()
	}

	args := flag.Args()
	if len(args) == 0 || filePath == nil || 0 == len(*filePath) {
		usage()
		os.Exit(eUsage)
		return
	}

	zkconn, _, err := zk.Connect([]string{*zkaddr}, zkTIMEOUT)
	defer zkconn.Close()
	if err != nil {
		log.Println("Failed to connect", err)
		os.Exit(eFail)
	}

	if cmd, ok := commands[args[0]]; ok {
		if err := cmd.Func(zkconn, *rootPath, *filePath); err != nil {
			log.Println("Fail to run", cmd.Name, err)
			os.Exit(eFail)
		}
	} else {
		usage()
		os.Exit(eUsage)
		return
	}
}
