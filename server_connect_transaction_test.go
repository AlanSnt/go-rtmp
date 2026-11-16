package rtmp

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/handshake"
)

// Some clients (the PlayStation 5) send "connect" with transaction ID 0 and
// ignore an answer that carries another ID.
func TestServerRepliesToConnectWithTheClientTransactionID(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:")
	require.Nil(t, err)

	srv := NewServer(&ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *ConnConfig) {
			return conn, &ConnConfig{Handler: &DefaultHandler{}, Logger: logrus.StandardLogger()}
		},
	})
	defer srv.Close()
	go srv.Serve(l)

	conn, err := net.Dial("tcp", l.Addr().String())
	require.Nil(t, err)
	defer conn.Close()
	require.Nil(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	require.Nil(t, handshake.HandshakeWithServer(conn, conn, &handshake.Config{}))

	const transactionID = "\x00\x00\x00\x00\x00\x00\x00\x00" // the number 0
	connect := "\x02\x00\x07connect" + "\x00" + transactionID +
		"\x03\x00\x03app\x02\x00\x03app\x00\x00\x09" // {app: "app"}
	// One chunk on chunk stream 3: a command message (0x14) on stream 0.
	header := []byte{0x03, 0, 0, 0, 0, 0, byte(len(connect)), 0x14, 0, 0, 0, 0}
	_, err = conn.Write(append(header, connect...))
	require.Nil(t, err)

	result := []byte("\x02\x00\x07_result\x00")
	var answer []byte
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		answer = append(answer, buf[:n]...)
		if i := bytes.Index(answer, result); i >= 0 && len(answer) >= i+len(result)+8 {
			require.Equal(t, []byte(transactionID), answer[i+len(result):][:8])
			return
		}
		require.Nil(t, err, "no _result in the answer to connect")
	}
}
