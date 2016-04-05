package main

import (
	"bufio"
	"bytes"
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

// AppVersion app version
const AppVersion = "Version 0.1.20160305"

// Version 0.0.201512141923  : the first version
// Version 0.1.20160305 : 增加多个集群的支持, 去掉log的配置,修复bug

var cmdPath string
var c Conf

var (
	// SUDOUSER sudo切换的用户
	SUDOUSER = os.Getenv("SUDO_USER")
	// USERNAME 本地运行的账户
	USERNAME = os.Getenv("username")
	// HOME $HOME 位置
	HOME       = os.Getenv("HOME")
	configFile = flag.String("C", "mkafka.yaml", "configure file.")
	usage      = `
  Examples:
  monitor_kafka_offset -C mkafka.yaml
`
)

// MKafka Monitor Kafka程序的主要数据结构
type MKafka struct {
	ZKList    []string
	ZNode     string
	ToolDir   string
	LogFormat string
	zkc       *zk.Conn
	SetLag    map[string]int64
	Cluster   string

	JobList      []ConsumerTopic
	Concurrent   int
	TimeWait     time.Duration
	TimeInterval time.Duration
	Collect      time.Duration
}

// Job 任务数据结构
type Job struct {
	jobname ConsumerTopic
	results chan<- Result
}

// ConsumerTopic consumer and Topic as a job
type ConsumerTopic struct {
	Consumer string
	Topic    string
}

// Result job处理结果
type Result struct {
	jobname    string
	resultcode int
	resultinfo string
}

// Offset 脚本运行后的数据结构
type Offset struct {
	Group   string
	Topic   string
	Pid     int
	Offset  int64
	LogSize int64
	Lag     int64
	Owner   string
}

// Conf 配置文件的数据结构
type Conf struct {
	ENV     Env              `yaml:"ENV"`
	LOG     Log              `yaml:"LOG"`
	Monitor []string         `yaml:"Monitor"`
	Cluster map[string]Kafka `yaml:"Cluster"`
}

// Env 服务运行的一交
type Env struct {
	KafkaToolDir string        `yaml:"KafkaToolDir"`
	JAVAHOME     string        `yaml:"JAVAHOME"`
	Concurrent   int           `yaml:"Concurrent"`
	TimeWait     time.Duration `yaml:"TimeWait"`
	TimeInterval time.Duration `yaml:"TimeInterval"`
	Collect      time.Duration `yaml:"Collect"`
}

// Log 配置文件中日志格式
type Log struct {
	LogFormat string `yaml:"LogFormat"`
}

// Kafka 连接Kafka的数据结构
type Kafka struct {
	ZNode  string   `yaml:"ZNode"`
	ZKList []string `yaml:"ZKList"`
	SetLag []Lag    `yaml:"SetLag"`
}

// Lag consumer的延迟数据结构
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
	kafka, t := NewMKafka()

	for {

		for n, m := range kafka {

			log.Tracef("[%v] start collect kafka information %v \n", n, m)

			realMain(m)

		}
		time.Sleep(t)
	}
}

func realMain(mkafka MKafka) {

	consumers, err := GetConsumsers(mkafka.ZNode, mkafka.zkc)
	if err != nil {
		log.Errorf("get consumers %v error: %v", mkafka.ZNode, err)
	} else {
		for _, consumer := range consumers {
			topics, err := GetTopicsWithConsumser(mkafka.ZNode, mkafka.zkc, consumer)
			if err != nil {
				log.Errorf("get topic of consumer %v:%v failed , error %v", mkafka.ZNode, consumer, err)
			} else {
				for _, topic := range topics {
					job := ConsumerTopic{
						Topic:    topic,
						Consumer: consumer,
					}
					mkafka.JobList = append(mkafka.JobList, job)
				}
			}
		}
	}
	//控制并发数量.
	doRequest(mkafka)

}

// NewMKafka start monitor kafka service
func NewMKafka() ([]MKafka, time.Duration) {
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
			ZKList:       c.Cluster[m].ZKList,
			ZNode:        c.Cluster[m].ZNode,
			ToolDir:      c.ENV.KafkaToolDir,
			LogFormat:    c.LOG.LogFormat,
			zkc:          zkcon,
			SetLag:       lags,
			Cluster:      m,
			TimeWait:     c.ENV.TimeWait,
			TimeInterval: c.ENV.TimeInterval,
			Concurrent:   c.ENV.Concurrent,
		}
		log.Infof("Load yaml conf %+v", f)
		infos = append(infos, f)
	}

	log.Infof("parse config file : %v", infos)
	return infos, c.ENV.Collect
}

// Do get consumers of topic offset
func (mk MKafka) Do(job *Job) {

	var outerr bytes.Buffer

	// export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
	os.Setenv("JAVA_HOME", c.ENV.JAVAHOME)
	cmd := exec.Command(mk.ToolDir+"/kafka-consumer-offset-checker.sh", "--zookeeper", strings.Join(mk.ZKList, ",")+mk.ZNode, "--topic", job.jobname.Topic, "--group", job.jobname.Consumer)

	jobstring := fmt.Sprintf("zkList:%v:%v --topic %v --group %v", strings.Join(mk.ZKList, ","), mk.ZNode, job.jobname.Topic, job.jobname.Consumer)

	//命令执行
	stdout, err := cmd.StdoutPipe()
	checkErr(2, err)
	stderr, err := cmd.StderrPipe()
	checkErr(2, err)

	err = cmd.Start()
	checkErr(2, err)

	done := make(chan error)

	go func() {
		done <- cmd.Wait()
	}()

	//直接输出
	//go io.Copy(&out, stdout)
	go io.Copy(&outerr, stderr)

	// Non-blockingly echo command output to terminal
	//go io.Copy(os.Stderr, stderr)
	//go io.Copy(os.Stdout, stdout)

	scanner := bufio.NewScanner(stdout)
	var totalLag, totalMessages int64
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
				log.Errorf("change '%s' to int error %v", words, err)
				continue
			}

			if offset, err := strconv.ParseInt(words[3], 10, 64); err == nil {
				data.Offset = offset
			} else {
				log.Errorf("change '%s' to int64 error %v", words, err)
				continue
			}

			if logsize, err := strconv.ParseInt(words[4], 10, 64); err == nil {
				data.LogSize = logsize
			} else {
				log.Errorf("change '%s' to int64 error %v", words, err)
				continue
			}

			if lag, err := strconv.ParseInt(words[5], 10, 64); err == nil {
				data.Lag = lag
			} else {
				log.Errorf("change '%s' to int64 error %v", words, err)
				continue
			}

			totalMessages += data.LogSize
			totalLag += data.Lag
			partition++

			if m, ok := mk.SetLag[job.jobname.Consumer]; ok {
				if data.Lag > m {
					log.Warnf("[Consumer Detail Info] "+mk.LogFormat, data.Topic, data.Group, data.Pid, data.LogSize, data.Offset, data.Lag, mk.Cluster)
				}
			} else if data.Lag > 50 {
				log.Warnf("[Consumer Detail Info] "+mk.LogFormat, data.Topic, data.Group, data.Pid, data.LogSize, data.Offset, data.Lag, mk.Cluster)
			}
		}
	}
	log.Infof("[Total Info] Topic:%v Consumer:%v Partition:%v Total_Messages:%v Total_Lag:%v Cluster:%s", job.jobname.Topic, job.jobname.Consumer, partition, totalMessages, totalLag, mk.Cluster)

	//线程控制执行时间
	select {
	case <-time.After(mk.TimeWait):
		log.Errorf("[TimeOver] job: %v ", jobstring)
		//超时被杀时
		if err := cmd.Process.Kill(); err != nil {
			//超时被杀失败
			job.results <- Result{jobstring, 3, "Killed..."}
			checkErr(2, err)
		}
		<-done
		job.results <- Result{jobstring, 2, "Time over ,Killed..."}
		//记录失败job
	case err := <-done:
		if err != nil {
			//完成返回失败
			job.results <- Result{jobstring, 1, outerr.String()}
		} else {

			//完成返回成功
			job.results <- Result{jobstring, 0, "success"}

		}
	}

}

// NewZk connect zookeeper
func NewZk(zks []string) (*zk.Conn, error) {
	c, _, err := zk.Connect(zks, time.Second*1)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// GetConsumsers 获取consumer的列表
func GetConsumsers(zknode string, zkc *zk.Conn) ([]string, error) {
	consumers, _, err := zkc.Children(zknode + "/consumers")
	return consumers, err
}

// GetTopicsWithConsumser 获取topic对应的consumer
func GetTopicsWithConsumser(zknode string, zkc *zk.Conn, consumer string) ([]string, error) {
	getzknode := zknode + "/consumers/" + consumer + "/offsets"
	topics, _, err := zkc.Children(getzknode)
	return topics, err
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

	// LogConfig seelog的配置文件
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

//####################
//并发调度过程
//处理job对列
//并发调度开始
func doRequest(mk MKafka) {
	jobs := make(chan Job, mk.Concurrent)
	results := make(chan Result, len(mk.JobList))
	done := make(chan struct{}, mk.Concurrent)

	log.Tracef("[%v] get jobList %v Concurrent %v \n", mk.Cluster, len(mk.JobList), mk.Concurrent)
	go mk.addJob(jobs, mk.JobList, results)

	for i := 0; i < mk.Concurrent; i++ {
		go mk.doJob(done, jobs)
	}

	go mk.awaitCompletion(done, results, mk.Concurrent)
}

//添加job
func (mk MKafka) addJob(jobs chan<- Job, jobnames []ConsumerTopic, results chan<- Result) {
	for _, jobname := range jobnames {
		jobs <- Job{jobname, results}
	}
	close(jobs)
}

//处理job
func (mk MKafka) doJob(done chan<- struct{}, jobs <-chan Job) {

	for job := range jobs {
		mk.Do(&job)
		time.Sleep(mk.TimeInterval)
	}
	done <- struct{}{}
}

//job完成状态
func (mk MKafka) awaitCompletion(done <-chan struct{}, results chan Result, works int) {
	for i := 0; i < works; i++ {
		<-done
	}
	close(results)
}
