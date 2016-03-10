package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	//"github.com/robfig/config"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/yaml.v2"
)

const AppVersion = "Version 0.1.20160305"

// Version 0.0.201512141923  : the first version
// Version 0.1.20160305 : 增加多个集群的支持, 去掉log的配置,修复bug

var cmdPath string
var c Conf

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
	ZKList    []string
	ZNode     string
	ToolDir   string
	LogFormat string
	Topics    []string
	Consumers []string
	zkc       *zk.Conn
	SetLag    map[string]int64
	Cluster   string
}

type Offset struct {
	Group   string
	Topic   string
	Pid     int
	Offset  int64
	LogSize int64
	Lag     int64
	Owner   string
}

type Conf struct {
	ENV     Env              `yaml:"ENV"`
	LOG     Log              `yaml:"LOG"`
	Monitor []string         `yaml:"Monitor"`
	Cluster map[string]Kafka `yaml:"Cluster"`
}

type Env struct {
	KafkaToolDir string `yaml:"KafkaToolDir"`
	JAVAHOME     string `yaml:"JAVAHOME"`
}

type Log struct {
	LogFormat string `yaml:"LogFormat"`
}

type Kafka struct {
	ZNode  string   `yaml:"ZNode"`
	ZKList []string `yaml:"ZKList"`
	SetLag []Lag    `yaml:"SetLag"`
}

type Lag struct {
	Consumer string `yaml:"Consumer"`
	Lag      int64  `yaml:"Lag"`
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	//日志记录
	cmdPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
	defer log.Flush()
	syncLogger()

	//获取相关信息
	kafka := NewMKafka()

	for {
		realMain(kafka)
		time.Sleep(30 * time.Second)
	}
}

func realMain(m []MKafka) {

	//根据consumer检查offset
	for _, v := range m {

		v.Consumers = GetConsumsers(v.ZNode, v.zkc)

		for _, consumer := range v.Consumers {
			log.Infof("get consumser %v", consumer)
			go v.checkConsumer(consumer)
		}

	}
}

func NewMKafka() []MKafka {
	flag.Parse()

	//配置文件
	var confFile string
	if !filepath.IsAbs(*configFile) {
		confFile = fmt.Sprintf("%s/../%s", cmdPath, *configFile)
	}
	yamlFile, err := ioutil.ReadFile(confFile)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		panic(err)
	}

	if len(c.Monitor) == 0 {
		fmt.Printf("Not found monitor cluser ,Please check conf %s\n", confFile)
		os.Exit(1)
	}

	var infos []MKafka

	for _, m := range c.Monitor {

		zkcon, err := NewZk(c.Cluster[m].ZKList)
		//TODO: disable panic
		checkErr(1, err)
		if err != nil {
			panic(err)
		}

		lags := make(map[string]int64)
		for _, v := range c.Cluster[m].SetLag {
			lags[v.Consumer] = v.Lag
		}

		f := MKafka{
			ZKList:    c.Cluster[m].ZKList,
			ZNode:     c.Cluster[m].ZNode,
			ToolDir:   c.ENV.KafkaToolDir,
			LogFormat: c.LOG.LogFormat,
			zkc:       zkcon,
			SetLag:    lags,
			Cluster:   m,
		}
		log.Infof("Load yaml conf %+v", f)
		infos = append(infos, f)
	}

	log.Infof("parse config file : %v", infos)
	return infos
}

func (mk MKafka) checkConsumer(consumer string) {

	topics := GetTopicsWithConsumser(mk.ZNode, mk.zkc, consumer)

	for _, topic := range topics {
		log.Infof("get topic %v ", topic)
		mk.getConsumerOffset(consumer, topic)
	}

}

func (mk MKafka) getConsumerOffset(consumer, topic string) {
	//var stdout, outerr bytes.Buffer
	// export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
	os.Setenv("JAVA_HOME", c.ENV.JAVAHOME)
	cmd := exec.Command(mk.ToolDir+"/kafka-consumer-offset-checker.sh", "--zookeeper", strings.Join(mk.ZKList, ",")+mk.ZNode, "--topic", topic, "--group", consumer)

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

			data := &Offset{
				Group: words[0],
				Topic: words[1],
				Owner: words[6],
			}

			if pid, err := strconv.Atoi(words[2]); err == nil {
				data.Pid = pid
			} else {
				log.Errorf("change %s to int error %v", words[2], err)
				continue
			}

			if offset, err := strconv.ParseInt(words[3], 10, 64); err == nil {
				data.Offset = offset
			} else {
				log.Errorf("change %s to int64 error %v", words[3], err)
				continue
			}

			if logsize, err := strconv.ParseInt(words[4], 10, 64); err == nil {
				data.LogSize = logsize
			} else {
				log.Errorf("change %s to int64 error %v", words[4], err)
				continue
			}

			if lag, err := strconv.ParseInt(words[5], 10, 64); err == nil {
				data.Lag = lag
			} else {
				log.Errorf("change %s to int64 error %v", words[5], err)
				continue
			}

			total_messages += data.LogSize
			total_lag += data.Lag
			partition += 1

			if m, ok := mk.SetLag[consumer]; ok {
				if data.Lag > m {
					log.Warnf("[Consumer Detail Info] "+mk.LogFormat, data.Topic, data.Group, data.Pid, data.LogSize, data.Offset, data.Lag, mk.Cluster)
				}
			} else if data.Lag > 50 {
				log.Warnf("[Consumer Detail Info] "+mk.LogFormat, data.Topic, data.Group, data.Pid, data.LogSize, data.Offset, data.Lag, mk.Cluster)
			}
		}
	}
	log.Infof("[Total Info] Topic:%v Consumer:%v Partition:%v Total_Messages:%v Total_Lag:%v Cluster:%s", topic, consumer, partition, total_messages, total_lag, mk.Cluster)
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
	getzknode := zknode + "/consumers/" + consumer + "/offsets"
	topics, _, err := zkc.Children(getzknode)
	if err != nil {
		log.Errorf("get zknode %s error %v", getzknode, err)
	}
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

func syncLogger() {

	var LogConfig = `
<seelog minlevel="info">
    <outputs formatid="common">
        <rollingfile type="date" filename="` + cmdPath + `/../log/mkafka.log" datepattern="2006010215" maxrolls="30"/>
        <filter levels="critical">
            <file path="` + cmdPath + `/../log/mkafka.log.wf" formatid="critical"/>
        </filter>
    </outputs>
    <formats>
        <format id="colored"  format="%Time %EscM(46)%Level%EscM(49) %Msg%n%EscM(0)"/>
        <format id="common" format="%Date/%Time [%LEV] [%File:%Line] %Msg%n" />
        <format id="critical" format="%Date/%Time %File:%Line %Func %Msg%n" />
    </formats>
</seelog>
`

	logger, _ := log.LoggerFromConfigAsBytes([]byte(LogConfig))
	log.UseLogger(logger)
}
