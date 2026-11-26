//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

func TestStreamerSingleChunk(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 2
	videoContent := []byte("testtesttest")
	msg := &message.VideoMessage{
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)
	require.NotNil(t, w)

	enc := message.NewEncoder(w)
	err = enc.Encode(msg)
	require.Nil(t, err)
	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	err = streamer.Sched(w)
	require.Nil(t, err)

	_, err = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	// read a message
	r, err := streamer.readChunk()
	require.Nil(t, err)
	require.NotNil(t, r)
	require.True(t, r.completed)

	dec := message.NewDecoder(r)
	var actualMsg message.Message
	err = dec.Decode(message.TypeID(r.messageTypeID), &actualMsg)
	require.Nil(t, err)
	require.Equal(t, uint32(timestamp), r.timestamp)

	// check message
	require.Equal(t, actualMsg.TypeID(), msg.TypeID())
	actualMsgT := actualMsg.(*message.VideoMessage)
	actualContent, _ := ioutil.ReadAll(actualMsgT.Payload)
	require.Equal(t, actualContent, videoContent)
}

func TestStreamerMultipleChunk(t *testing.T) {
	const chunkSize = 128
	const payloadUnit = "test"

	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 2
	videoContent := []byte(strings.Repeat(payloadUnit, chunkSize))
	msg := &message.VideoMessage{
		// will be chunked (chunkSize * len(payloadUnit))
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)
	require.NotNil(t, w)

	enc := message.NewEncoder(w)
	err = enc.Encode(msg)
	require.Nil(t, err)
	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	err = streamer.Sched(w)
	require.Nil(t, err)

	_, err = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	// read a message
	var r *ChunkStreamReader
	for i := 0; i < len(payloadUnit); i++ {
		r, err = streamer.readChunk()
		require.Nil(t, err)
	}
	require.NotNil(t, r)

	dec := message.NewDecoder(r)
	var actualMsg message.Message
	err = dec.Decode(message.TypeID(r.messageTypeID), &actualMsg)
	require.Nil(t, err)
	require.Equal(t, uint32(timestamp), r.timestamp)

	// check message
	require.Equal(t, actualMsg.TypeID(), msg.TypeID())
	actualMsgT := actualMsg.(*message.VideoMessage)
	actualContent, _ := ioutil.ReadAll(actualMsgT.Payload)
	require.Equal(t, actualContent, videoContent)
}

func TestStreamerChunkExample1(t *testing.T) {
	type write struct {
		timestamp uint32
		length    int
	}

	type read struct {
		timestamp  uint32
		fmt        byte
		isComplete bool
	}

	type testCase struct {
		name            string
		chunkStreamID   int
		typeID          byte
		messageStreamID uint32
		writeCases      []write
		readCases       []read
	}

	tcs := []testCase{
		// Example #1
		{
			name:            "Example #1",
			chunkStreamID:   3,
			typeID:          8,
			messageStreamID: 12345,
			writeCases: []write{
				{timestamp: 1000, length: 32},
				{timestamp: 1020, length: 32},
				{timestamp: 1040, length: 32},
				{timestamp: 1060, length: 32},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0, isComplete: true},
				{timestamp: 1020, fmt: 2, isComplete: true},
				{timestamp: 1040, fmt: 3, isComplete: true},
				{timestamp: 1060, fmt: 3, isComplete: true},
			},
		},
		// Example #2
		{
			name:            "Example #2",
			chunkStreamID:   4,
			typeID:          9,
			messageStreamID: 12346,
			writeCases: []write{
				{timestamp: 1000, length: 307},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0},
				{timestamp: 1000, fmt: 3},
				{timestamp: 1000, fmt: 3, isComplete: true},
			},
		},
		// Original #1 fmt0 -> fmt3, fmt2 -> fmt3
		{
			name:            "Original #1",
			chunkStreamID:   5,
			typeID:          10,
			messageStreamID: 22346,
			writeCases: []write{
				{timestamp: 1000, length: 200},
				{timestamp: 2000, length: 200},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0},
				{timestamp: 1000, fmt: 3, isComplete: true},
				{timestamp: 1000, fmt: 2}, // timestamp delta is not updated in this time
				{timestamp: 2000, fmt: 3, isComplete: true},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 2048))
			inbuf := bufio.NewReaderSize(buf, 2048)
			outbuf := bufio.NewWriterSize(buf, 2048)

			streamer := NewChunkStreamer(inbuf, outbuf, nil)

			for i, wc := range tc.writeCases {
				t.Run(fmt.Sprintf("Write: %d", i), func(t *testing.T) {
					w, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID)
					require.Nil(t, err)
					require.NotNil(t, w)

					bin := make([]byte, wc.length)

					w.messageLength = uint32(len(bin))
					w.messageTypeID = tc.typeID
					w.messageStreamID = tc.messageStreamID
					w.timestamp = wc.timestamp
					w.buf.Write(bin)

					err = streamer.Sched(w)
					require.Nil(t, err)
				})
			}

			_, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID) // wait for writing
			require.Nil(t, err)

			for i, rc := range tc.readCases {
				t.Run(fmt.Sprintf("Read: %d", i), func(t *testing.T) {
					r, err := streamer.readChunk()
					require.Nil(t, err)
					require.NotNil(t, r)

					require.Equal(t, rc.fmt, r.basicHeader.fmt)
					require.Equal(t, uint32(rc.timestamp), r.timestamp)
					require.Equal(t, rc.isComplete, r.completed)
				})
			}
		})
	}
}

func TestStreamerChunkExample2(t *testing.T) {
	type write struct {
		timestamp     uint32
		length        int
		messageTypeId byte
	}

	type read struct {
		timestamp  uint32
		delta      uint32
		fmt        byte
		isComplete bool
	}

	type testCase struct {
		name            string
		chunkStreamID   int
		messageStreamID uint32
		writeCases      []write
		readCases       []read
	}

	tcs := []testCase{
		// Same timestamp
		{
			name:            "Same timestamp's delta #1",
			chunkStreamID:   5,
			messageStreamID: 22346,
			writeCases: []write{
				{timestamp: 1000, length: 200, messageTypeId: 10},
				{timestamp: 1001, length: 200, messageTypeId: 11},
				{timestamp: 2000, length: 200, messageTypeId: 10},
				{timestamp: 2000, length: 200, messageTypeId: 11},
			},
			readCases: []read{
				{timestamp: 1000, delta: 0, fmt: 0, isComplete: false},
				{timestamp: 1000, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 1000, delta: 1, fmt: 1, isComplete: false},
				{timestamp: 1001, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 1001, delta: 999, fmt: 1, isComplete: false},
				{timestamp: 2000, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 2000, delta: 0, fmt: 1, isComplete: false},
				{timestamp: 2000, delta: 0, fmt: 3, isComplete: true},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 2048))
			inbuf := bufio.NewReaderSize(buf, 2048)
			outbuf := bufio.NewWriterSize(buf, 2048)

			streamer := NewChunkStreamer(inbuf, outbuf, nil)

			for i, wc := range tc.writeCases {
				t.Run(fmt.Sprintf("Write: %d", i), func(t *testing.T) {
					w, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID)
					require.Nil(t, err)
					require.NotNil(t, w)

					bin := make([]byte, wc.length)

					w.messageLength = uint32(len(bin))
					w.messageTypeID = byte(wc.messageTypeId)
					w.messageStreamID = tc.messageStreamID
					w.timestamp = wc.timestamp
					w.buf.Write(bin)
					err = streamer.Sched(w)
					require.Nil(t, err)
				})
			}

			_, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID) // wait for writing
			require.Nil(t, err)

			for i, rc := range tc.readCases {
				t.Run(fmt.Sprintf("Read: %d", i), func(t *testing.T) {
					r, err := streamer.readChunk()
					_ = rc
					_ = err
					require.Nil(t, err)
					require.NotNil(t, r)
					require.Equal(t, rc.fmt, r.basicHeader.fmt)
					require.Equal(t, uint32(rc.delta), r.messageHeader.timestampDelta)
					require.Equal(t, rc.isComplete, r.completed)
				})
			}
		})
	}
}

func TestWriteToInvalidWriter(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, &AlwaysErrorWriter{}, nil)

	// Write some data
	chunkStreamID := 10
	timestamp := uint32(0)
	err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
		StreamID: 0,
		Message:  &message.Ack{},
	})
	require.Nil(t, err)

	<-streamer.Done()
	require.EqualErrorf(t, streamer.Err(), "Always error!", "")
}

type AlwaysErrorWriter struct{}

func (w *AlwaysErrorWriter) Write(buf []byte) (int, error) {
	return 0, fmt.Errorf("Always error!")
}

func TestChunkStreamerHasNoLeaksOfGoroutines(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
}

func TestChunkStreamerStreamsLimitation(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, &StreamControlStateConfig{
		MaxChunkStreams: 1,
	})
	defer streamer.Close()

	{
		_, err := streamer.prepareChunkReader(0)
		require.Nil(t, err)

		_, err = streamer.prepareChunkReader(1)
		require.EqualError(t, err, "Creating chunk streams limit exceeded(Reader): Limit = 1")
	}

	{
		_, err := streamer.prepareChunkWriter(0)
		require.Nil(t, err)

		_, err = streamer.prepareChunkWriter(1)
		require.EqualError(t, err, "Creating chunk streams limit exceeded(Writer): Limit = 1")
	}
}

func TestChunkStreamerDualWriter(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	largePayload := []byte(strings.Repeat("abcdabcd12341234", 512))

	// Writes messages alternately to two streams 20 times without clogging
	const N = 20
	for i := 0; i < N; i++ {
		chunkStreamID := 10 + i%2

		// Write some data
		timestamp := uint32(0)
		err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
			StreamID: 0,
			Message: &message.VideoMessage{
				Payload: bytes.NewReader(largePayload),
			},
		})
		require.Nil(t, err)
	}

	streamer.waitWriters()

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func TestChunkStreamerDualWriterWithoutWaiting(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	largePayload := []byte(strings.Repeat("abcdabcd12341234", 512))

	// Writes messages alternately to two streams 20 times without clogging
	const N = 20
	for i := 0; i < N; i++ {
		chunkStreamID := 10 + i%2

		// Write some data
		timestamp := uint32(0)
		err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
			StreamID: 0,
			Message: &message.VideoMessage{
				Payload: bytes.NewReader(largePayload),
			},
		})
		require.Nil(t, err)
	}

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func TestChunkStreamerNewChunkWriterTwice(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 10

	_, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = streamer.NewChunkWriter(ctx, chunkStreamID) // Try to acquire same chunk writer
	require.EqualError(t, err, "Failed to wait chunk writer: context deadline exceeded")

	err = streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func BenchmarkStreamerMultipleChunkRead(b *testing.B) {
	const chunkSize = 128
	const payloadUnit = "test"

	buf := bytes.NewBuffer(nil)
	streamer := NewChunkStreamer(buf, buf, nil)

	chunkStreamID := 2
	videoContent := []byte(strings.Repeat(payloadUnit, chunkSize))
	msg := &message.VideoMessage{
		// will be chunked (chunkSize * len(payloadUnit))
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, _ := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	enc := message.NewEncoder(w)
	_ = enc.Encode(msg)

	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	_ = streamer.Sched(w)

	_, _ = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing

	r := bytes.NewReader(buf.Bytes())
	s := NewChunkStreamer(r, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(buf.Bytes())

		_, err := s.NewChunkReader()
		if err != nil {
			b.Error(err)
		}
	}
}

func TestStreamerSkipsEmptyMessagesOfAnyType(t *testing.T) {
	chunk := func(typeID message.TypeID, payload string) []byte {
		header := []byte{
			0x04,             // fmt 0, chunk stream 4
			0x00, 0x00, 0x00, // timestamp
			0x00, 0x00, byte(len(payload)),
			byte(typeID),
			0x01, 0x00, 0x00, 0x00, // message stream 1
		}
		return append(header, payload...)
	}
	var in []byte
	for _, typeID := range []message.TypeID{
		message.TypeIDVideoMessage,
		message.TypeIDAudioMessage,
		message.TypeIDDataMessageAMF0,
		message.TypeIDCommandMessageAMF0,
		message.TypeIDAck,
	} {
		in = append(in, chunk(typeID, "")...)
	}
	in = append(in, chunk(message.TypeIDVideoMessage, "next")...)
	streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(in)), bufio.NewWriter(ioutil.Discard), nil)

	var next ChunkMessage
	_, _, err := streamer.Read(&next)
	require.Nil(t, err)
	content, _ := ioutil.ReadAll(next.Message.(*message.VideoMessage).Payload)
	require.Equal(t, "next", string(content))
}

func TestStreamerRejectsAMessageShorterThanWhatItAlreadyHolds(t *testing.T) {
	firstChunk := append([]byte{
		0x04,
		0x00, 0x00, 0x00,
		0x00, 0x00, 200, // 200 bytes: the default 128-byte chunk leaves it unfinished
		byte(message.TypeIDVideoMessage),
		0x01, 0x00, 0x00, 0x00,
	}, bytes.Repeat([]byte{0xAA}, 128)...)
	shorterChunk := []byte{
		0x44, // fmt 1, chunk stream 4
		0x00, 0x00, 0x00,
		0x00, 0x00, 10,
		byte(message.TypeIDVideoMessage),
	}
	in := append(firstChunk, shorterChunk...)
	streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(in)), bufio.NewWriter(ioutil.Discard), nil)

	var msg ChunkMessage
	_, _, err := streamer.Read(&msg)
	require.EqualError(t, err, "chunk stream 4 holds 128 bytes of a 10-byte message")
}

// Past 0xFFFFFF ms (4h39m) the timestamp moves to the extended field, and every
// fmt 3 continuation of that message repeats it: reading those 4 bytes as payload
// shifted the stream and cut it.
func TestStreamerReadsTheExtendedTimestampOfContinuationChunks(t *testing.T) {
	extended := []byte{0x01, 0x00, 0x00, 0x00}
	in := []byte{
		0x04,             // fmt 0, chunk stream 4
		0xff, 0xff, 0xff, // timestamp in the extended field
		0x00, 0x00, 200, // 200 bytes: the default 128-byte chunk splits it
		byte(message.TypeIDVideoMessage),
		0x01, 0x00, 0x00, 0x00,
	}
	in = append(in, extended...)
	in = append(in, bytes.Repeat([]byte{0xAA}, 128)...)
	in = append(in, 0xC4) // fmt 3, chunk stream 4
	in = append(in, extended...)
	in = append(in, bytes.Repeat([]byte{0xBB}, 72)...)
	streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(in)), bufio.NewWriter(ioutil.Discard), nil)

	var msg ChunkMessage
	_, timestamp, err := streamer.Read(&msg)
	require.Nil(t, err)
	require.Equal(t, uint32(0x01000000), timestamp)
	content, _ := ioutil.ReadAll(msg.Message.(*message.VideoMessage).Payload)
	require.Equal(t, append(bytes.Repeat([]byte{0xAA}, 128), bytes.Repeat([]byte{0xBB}, 72)...), content)
}
