package main

import (
	"bufio"
	//"bytes"
	"flag"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"github.com/robfig/config"
	"github.com/samuel/go-zookeeper/zk"
)

const AppVersion = "Version 0.0.201512141923"

var (
	SUDO_USER  = os.Getenv("SUDO_USER")
	USERNAME   = os.Getenv("username")
	HOME       = os.Getenv("HOME")
	configFile = flag.String("C", "mkafka.conf", "configure file.")
	usage      = `
  Examples:
  monitor_kafka_offset -C mkafka.conf
`
)

type MKafka struct {
	ZKList    string
	ZNode     string
	ToolDir   string
	LogFormat string
	Topics    []string
	Consumers []string
	zkc       *zk.Conn
	SetLag    map[string]int64
}

type Offset struct {
	Group   string
	Topic   string
	Pid     int
	Offset  int64
	logSize int64
	Lag     int64
	Owner   string
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	//获取相关信息
	kafka := NewMKafka()

	for {
		realMain(kafka)
		time.Sleep(30 * time.Second)
	}
}

func realMain(m *MKafka) {

	//根据consumer检查offset

	m.Consumers = GetConsumsers(m.ZNode, m.zkc)

	for _, consumer := range m.Consumers {
		log.Infof("get consumser %v", consumer)
		go m.checkConsumer(consumer)
	}
}

func NewMKafka() *MKafka {
	flag.Parse()
	//配置文件
	conf, err := config.ReadDefault(*configFile)
	checkErr(1, err)

	//日志记录
	logConf, _ := conf.String("Log", "LogConf")
	logger, _ := log.LoggerFromConfigAsFile(logConf)
	log.ReplaceLogger(logger)

	zklist, err := conf.String("Monitor", "ZKList")
	checkErr(1, err)
	zknode, err := conf.String("Monitor", "ZNode")
	checkErr(1, err)

	tooldir, err := conf.String("Monitor", "KafkaToolDir")
	checkErr(1, err)
	logformat, err := conf.String("Log", "LogFormat")
	checkErr(1, err)

	lags, err := conf.String("Monitor", "SetLag")
	checkErr(1, err)

	setlag := make(map[string]int64)

	topic_lags := strings.Split(lags, ",")
	for _, v := range topic_lags {
		k := strings.Split(v, ":")
		if len(k) == 2 {
			setlag[k[0]], err = strconv.ParseInt(k[1], 10, 64)
			checkErr(1, err)
		}
	}

	zks := strings.Split(zklist, ",")
	zkcon, err := NewZk(zks)
	checkErr(1, err)

	info := &MKafka{
		ZKList:    zklist,
		ZNode:     zknode,
		ToolDir:   tooldir,
		LogFormat: logformat,
		zkc:       zkcon,
		SetLag:    setlag,
	}

	//info.Consumers = GetConsumsers(zknode, zkcon)
	return info
}

func (mk *MKafka) checkConsumer(consumer string) {

	topics := GetTopicsWithConsumser(mk.ZNode, mk.zkc, consumer)

	for _, topic := range topics {
		log.Infof("get topic %v ", topic)
		mk.getConsumerOffset(consumer, topic)
	}

}

func (mk *MKafka) getConsumerOffset(consumer, topic string) {
	//var stdout, outerr bytes.Buffer
	// export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
	os.Setenv("JAVA_HOME", "/usr/java/jdk1.7.0_67-cloudera")
	cmd := exec.Command(mk.ToolDir+"/kafka-consumer-offset-checker.sh", "--zookeeper", mk.ZKList+mk.ZNode, "--topic", topic, "--group", consumer)

	//命令执行
	stdout, err := cmd.StdoutPipe()
	checkErr(2, err)
	stderr, err := cmd.StderrPipe()
	checkErr(2, err)

	err = cmd.Start()
	checkErr(2, err)

	defer cmd.Wait() // Doesn't block

	// Non-blockingly echo command output to terminal
	go io.Copy(os.Stderr, stderr)
	//go io.Copy(os.Stdout, stdout)

	scanner := bufio.NewScanner(stdout)

	var total_lag, total_messages int64
	partition := 0
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		if len(words) == 7 {
			if words[0] == "Group" && words[1] == "Topic" {
				continue
			}

			pid, err := strconv.Atoi(words[2])
			checkErr(1, err)

			offset, err := strconv.ParseInt(words[3], 10, 64)
			checkErr(1, err)

			logsize, err := strconv.ParseInt(words[4], 10, 64)
			checkErr(1, err)

			lag, err := strconv.ParseInt(words[5], 10, 64)
			checkErr(1, err)

			data := &Offset{
				Group:   words[0],
				Topic:   words[1],
				Pid:     pid,
				Offset:  offset,
				logSize: logsize,
				Lag:     lag,
				Owner:   words[6],
			}

			total_messages += logsize
			total_lag += lag
			partition += 1

			if m, ok := mk.SetLag[consumer]; ok {
				if data.Lag > m {
					log.Warnf(mk.LogFormat, data.Topic, data.Group, data.Pid, data.logSize, data.Offset, data.Lag)
				}
			} else if data.Lag > 50 {
				//"topic:%s consumer:%s partition:%d logsize:%s offset:%s lag:%s"
				log.Warnf(mk.LogFormat, data.Topic, data.Group, data.Pid, data.logSize, data.Offset, data.Lag)

			}
		}
	}
	log.Infof("[Total Info] Topic:%s Consumer:%s Partition:%d Total_Messages:%v Total_Lag:%v", topic, consumer, partition, total_messages, total_lag)
}

func NewZk(zks []string) (*zk.Conn, error) {
	c, _, err := zk.Connect(zks, time.Second*1)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func GetConsumsers(zknode string, zkc *zk.Conn) []string {
	consumers, _, err := zkc.Children(zknode + "/consumers")
	checkErr(1, err)
	return consumers

}

func GetTopicsWithConsumser(zknode string, zkc *zk.Conn, consumer string) []string {
	topics, _, err := zkc.Children(zknode + "/consumers/" + consumer + "/offsets")
	checkErr(1, err)
	return topics
}

func checkErr(i int, err error) {
	if err != nil {
		switch i {
		case 1:
			log.Critical(err)
		case 2:
			log.Warn(err)
		default:
			log.Info(err)
		}
	}
	log.Flush()
}

//检查文件是否存在.
func checkExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
