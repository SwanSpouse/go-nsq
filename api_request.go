package nsq

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

type wrappedResp struct {
	Status     string      `json:"status_txt"`
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data"`
}

// stores the result in the value pointed to by ret(must be a pointer)
// HTTP请求
func apiRequestNegotiateV1(method string, endpoint string, body io.Reader, ret interface{}) error {
	// 在这里设置超时时间
	httpclient := &http.Client{Transport: newDeadlineTransport(2 * time.Second)}
	// 创建一个request
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}
	// 写入参数
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	// 发送请求
	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}
	// 读取返回值
	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	// 判断请求状态码
	if resp.StatusCode != 200 {
		return fmt.Errorf("got response %s %q", resp.Status, respBody)
	}
	// 看body是否为空，为空则搞一个空的json结构
	if len(respBody) == 0 {
		respBody = []byte("{}")
	}
	// 如果是version 1.0 版本的话这样操作
	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		return json.Unmarshal(respBody, ret)
	}
	// 不是1.0版本的要在外面包一层
	wResp := &wrappedResp{
		Data: ret,
	}
	if err = json.Unmarshal(respBody, wResp); err != nil {
		return err
	}
	// wResp.StatusCode here is equal to resp.StatusCode, so ignore it
	return nil
}
