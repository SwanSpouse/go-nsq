package nsq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Producer对接Tcp的接口抽象
type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	SetLoggerLevel(LogLevel)
	SetLoggerForLevel(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64        // 消费者id
	addr   string       // ip
	conn   producerConn // producer 的Tcp连接
	config Config       // 配置

	logger   []logger     // 不同log level的logger
	logLvl   LogLevel     //
	logGuard sync.RWMutex // setLogger的时候还需要控制并发

	responseChan chan []byte // 返回值 channel
	errorChan    chan []byte // error channel
	closeChan    chan int    // close channel

	transactionChan chan *ProducerTransaction // 所有的command都会转成一个事务
	transactions    []*ProducerTransaction    // 把接收到的事务添加到数组里
	state           int32                     // 状态

	concurrentProducers int32          // 并发的生产者
	stopFlag            int32          // 停止标识 这个东西只在启动和链接的时候用到了
	exitChan            chan int       // 退出channel
	wg                  sync.WaitGroup //
	guard               sync.Mutex     //
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd      *Command // 命令
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync 参数
}

// 当前事务结束标志
func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t // 向doneChan发送消息表示结束
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
// 在Config传递到NewProducer之后，这些配置的值是不可变的了
func NewProducer(addr string, config *Config) (*Producer, error) {
	// 确保config是通过NewConfig来创建的；如果不是的话会在这里panic
	// TODO @limingji 这里的这个assert是不是多余了；因为已经在config.Validate()里面进行过验证了
	// TODO @limingji 这个producer和consumer一样，都重复了
	config.assertInitialized()
	// 对配置项进行验证
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	// 往Producer里面塞各种值
	p := &Producer{
		id: atomic.AddInt64(&instCount, 1), // 生成producer id

		addr:   addr,    // ip
		config: *config, // 配置

		logger: make([]logger, int(LogLevelMax+1)),
		logLvl: LogLevelInfo, // 默认的log level 是info

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}

	// Set default logger for all log levels
	// 为不同log level塞上默认的logger
	l := log.New(os.Stderr, "", log.Flags())
	for index, _ := range p.logger {
		p.logger[index] = l
	}
	return p, nil
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
// 通过PING命令向Producer链接的NSQD发送一个命令；以便验证当前的Producer配置的是否正确；而不是说在Publish的时候才发现有问题。
func (w *Producer) Ping() error {
	// 首先验证状态是否是已连接
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}
	// 写入一个Nop命令
	return w.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	for level := range w.logger {
		w.logger[level] = l
	}
	w.logLvl = lvl
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (w *Producer) SetLoggerForLevel(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger[lvl] = l
}

// SetLoggerLevel sets the package logging level.
func (w *Producer) SetLoggerLevel(lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logLvl = lvl
}

func (w *Producer) getLogger(lvl LogLevel) (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger[lvl], w.logLvl
}

func (w *Producer) getLogLevel() LogLevel {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "stopping")
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) Publish(topic string, body []byte) error {
	return w.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommand(cmd)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (w *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return w.sendCommand(DeferredPublish(topic, delay, body))
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
// 这个怎么没有Multi呢？
func (w *Producer) DeferredPublishAsync(topic string, delay time.Duration, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error {
	return w.sendCommandAsync(DeferredPublish(topic, delay, body), doneChan, args)
}

// 同步发送命令
func (w *Producer) sendCommand(cmd *Command) error {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	t := <-doneChan
	return t.Error
}

// 异步发送命令
func (w *Producer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}
	// 构造一个事务
	t := &ProducerTransaction{
		cmd:      cmd,      // 命令
		doneChan: doneChan, // 结束标志
		Args:     args,     // 命令参数
	}
	// 在这里通过这种方式来进行同步、异步控制
	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}
	return nil
}

// 连接对应的NSQD
func (w *Producer) connect() error {
	// 首先加个锁
	w.guard.Lock()
	defer w.guard.Unlock()
	// 如果已经停止了， 则不进行连接了；返回一个错误
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}
	// 这里有一个状态转换自动机
	switch state := atomic.LoadInt32(&w.state); state {
	case StateInit:
	case StateConnected:
		return nil
	default:
		return ErrNotConnected
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", w.addr)

	// 在这里连接nsqd
	w.conn = NewConn(w.addr, &w.config, &producerConnDelegate{w})
	// 感觉有点儿复杂啊；这个功能加的
	w.conn.SetLoggerLevel(w.getLogLevel())
	for index := range w.logger {
		w.conn.SetLoggerForLevel(w.logger[index], LogLevel(index), fmt.Sprintf("%3d (%%s)", w.id))
	}
	// 尝试连接
	_, err := w.conn.Connect()
	if err != nil {
		w.conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", w.addr, err)
		return err
	}
	// 标记已经连接上了
	atomic.StoreInt32(&w.state, StateConnected)
	// 初始化closeChan
	w.closeChan = make(chan int)
	w.wg.Add(1)
	// 启动一个router
	go w.router()

	return nil
}

func (w *Producer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

// 在这里会和直接和Tcp来打交道
func (w *Producer) router() {
	// 死循环在这里不断的接受命令
	for {
		select {
		case t := <-w.transactionChan:
			// 接收到发送的命令、 事务
			// 把所有接受到的事务添加到数组里
			w.transactions = append(w.transactions, t)
			// 向TcpConn中写入命令
			err := w.conn.WriteCommand(t.cmd)
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", w.conn.String(), err)
				w.close()
			}
		case data := <-w.responseChan:
			// 多个command组成一组事务
			w.popTransaction(FrameTypeResponse, data)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "exiting router")
}

// 从transaction数组里面pop出一个事务出来进行处理
func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	// 不断的销毁transaction
	w.transactions = w.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
	}
	t.finish()
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	// 清空
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// 其实这里就是一个小的log包
func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

// 在这里把data写到responseChan里面
func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.close() }
func (w *Producer) onConnClose(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()
	close(w.closeChan)
}
