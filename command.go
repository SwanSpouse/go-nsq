package nsq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n") // 抽时间了解一下\r\n和\n的区别

// Command represents a command from a client to an NSQ daemon
type Command struct {
	Name   []byte   // 命令名称
	Params [][]byte // 参数名称
	Body   []byte   // body
}

// String returns the name and parameters of the Command
func (c *Command) String() string {
	if len(c.Params) > 0 {
		return fmt.Sprintf("%s %s", c.Name, string(bytes.Join(c.Params, byteSpace)))
	}
	return string(c.Name)
}

// WriteTo implements the WriterTo interface and
// serializes the Command to the supplied Writer.
//
// It is suggested that the target Writer is buffered
// to avoid performing many system calls.
<<<<<<< Updated upstream
// 格式是首部字段，然后Command名字，然后是空格分割的参数。然后是一个\n 然后才是body
/*
IDENTIFY(command name) PARAM1 PARAM2 ...\n
BODY_LENGTH(4Byte) BODY_CONTENT
*/
=======
// 写入一个命令
// 命令第一行是 名称 参数1 参数2 .... \n
// 命令第二行是 len-body
>>>>>>> Stashed changes
func (c *Command) WriteTo(w io.Writer) (int64, error) {
	// 记录此命令的总字节数
	var total int64
	var buf [4]byte
	// 首先写入命令名称
	n, err := w.Write(c.Name)
	total += int64(n)
	if err != nil {
		return total, err
	}
<<<<<<< Updated upstream
	// 各个param之间是以空格分割的。
=======
	// 写入参数；先写入空格、然后写入参数
>>>>>>> Stashed changes
	for _, param := range c.Params {
		n, err := w.Write(byteSpace)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(param)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
<<<<<<< Updated upstream
	// 写入\n
=======
	// 写入一个Newline
>>>>>>> Stashed changes
	n, err = w.Write(byteNewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}
<<<<<<< Updated upstream
	// 如果消息体内容不为空，则写入body
=======
	// 写入body
>>>>>>> Stashed changes
	if c.Body != nil {
		// 先写入长度、后写入body
		bufs := buf[:]
		// 首先写入body长度，4个字节
		binary.BigEndian.PutUint32(bufs, uint32(len(c.Body)))
		n, err := w.Write(bufs)
		total += int64(n)
		if err != nil {
			return total, err
		}
		// 再写入Body内容
		n, err = w.Write(c.Body)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Identify creates a new Command to provide information about the client.  After connecting,
// it is generally the first message sent.
//
// The supplied map is marshaled into JSON to provide some flexibility
// for this command to evolve over time.
//
// See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
// on the supported options
func Identify(js map[string]interface{}) (*Command, error) {
	// 这个传过去的是个json
	body, err := json.Marshal(js)
	if err != nil {
		return nil, err
	}
	return &Command{[]byte("IDENTIFY"), nil, body}, nil
}

// Auth sends credentials for authentication
//
// After `Identify`, this is usually the first message sent, if auth is used.
func Auth(secret string) (*Command, error) {
	return &Command{[]byte("AUTH"), nil, []byte(secret)}, nil
}

// Register creates a new Command to add a topic/channel for the connected nsqd
func Register(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("REGISTER"), params, nil}
}

// UnRegister creates a new Command to remove a topic/channel for the connected nsqd
func UnRegister(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("UNREGISTER"), params, nil}
}

// Ping creates a new Command to keep-alive the state of all the
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil, nil}
}

// Publish creates a new Command to write a message to a given topic
func Publish(topic string, body []byte) *Command {
	var params = [][]byte{[]byte(topic)}
	return &Command{[]byte("PUB"), params, body}
}

// DeferredPublish creates a new Command to write a message to a given topic
// where the message will queue at the channel level until the timeout expires
func DeferredPublish(topic string, delay time.Duration, body []byte) *Command {
	var params = [][]byte{[]byte(topic), []byte(strconv.Itoa(int(delay / time.Millisecond)))}
	return &Command{[]byte("DPUB"), params, body}
}

// MultiPublish creates a new Command to write more than one message to a given topic
// (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
func MultiPublish(topic string, bodies [][]byte) (*Command, error) {
	var params = [][]byte{[]byte(topic)}

	num := uint32(len(bodies))
	bodySize := 4
	for _, b := range bodies {
		bodySize += len(b) + 4
	}
	body := make([]byte, 0, bodySize)
	buf := bytes.NewBuffer(body)

	err := binary.Write(buf, binary.BigEndian, &num)
	if err != nil {
		return nil, err
	}
	for _, b := range bodies {
		err = binary.Write(buf, binary.BigEndian, int32(len(b)))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(b)
		if err != nil {
			return nil, err
		}
	}

	return &Command{[]byte("MPUB"), params, buf.Bytes()}, nil
}

// Subscribe creates a new Command to subscribe to the given topic/channel
func Subscribe(topic string, channel string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &Command{[]byte("SUB"), params, nil}
}

// Ready creates a new Command to specify
// the number of messages a client is willing to receive
func Ready(count int) *Command {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &Command{[]byte("RDY"), params, nil}
}

// Finish creates a new Command to indiciate that
// a given message (by id) has been processed successfully
func Finish(id MessageID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("FIN"), params, nil}
}

// Requeue creates a new Command to indicate that
// a given message (by id) should be requeued after the given delay
// NOTE: a delay of 0 indicates immediate requeue
func Requeue(id MessageID, delay time.Duration) *Command {
	var params = [][]byte{id[:], []byte(strconv.Itoa(int(delay / time.Millisecond)))}
	return &Command{[]byte("REQ"), params, nil}
}

// Touch creates a new Command to reset the timeout for
// a given message (by id)
func Touch(id MessageID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("TOUCH"), params, nil}
}

// StartClose creates a new Command to indicate that the
// client would like to start a close cycle.  nsqd will no longer
// send messages to a client in this state and the client is expected
// finish pending messages and close the connection
func StartClose() *Command {
	return &Command{[]byte("CLS"), nil, nil}
}

// Nop creates a new Command that has no effect server side.
// Commonly used to respond to heartbeats
func Nop() *Command {
	return &Command{[]byte("NOP"), nil, nil}
}
