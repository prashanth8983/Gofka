package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// RequestHandlerWrapper wraps the RequestHandler to handle the size prefix correctly
type RequestHandlerWrapper struct {
	handler *RequestHandler
}

// NewRequestHandlerWrapper creates a new wrapper
func NewRequestHandlerWrapper(broker BrokerInterface) *RequestHandlerWrapper {
	return &RequestHandlerWrapper{
		handler: NewRequestHandler(broker),
	}
}

// HandleRequestWithoutSize handles a request where the size has already been consumed
func (w *RequestHandlerWrapper) HandleRequestWithoutSize(msgBuf []byte) ([]byte, error) {
	// The msgBuf already has the message without the size prefix
	// We need to add it back for the handler
	fullMsg := make([]byte, 4+len(msgBuf))
	binary.BigEndian.PutUint32(fullMsg[0:4], uint32(len(msgBuf)))
	copy(fullMsg[4:], msgBuf)

	reader := bytes.NewReader(fullMsg)
	writer := &bytes.Buffer{}

	// The handler will read the header and route to the appropriate handler
	err := w.handler.HandleRequest(reader, writer)
	if err != nil {
		return nil, fmt.Errorf("handler error: %w", err)
	}

	// The handler writes just the response content without size prefix
	// We need to add the size prefix
	responseContent := writer.Bytes()
	response := make([]byte, 4+len(responseContent))
	binary.BigEndian.PutUint32(response[0:4], uint32(len(responseContent)))
	copy(response[4:], responseContent)

	return response, nil
}