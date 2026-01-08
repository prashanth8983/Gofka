package message

import "errors"

var (
	// ErrInvalidMessage indicates the message format is invalid
	ErrInvalidMessage = errors.New("invalid message format")

	// ErrCRCMismatch indicates the CRC validation failed
	ErrCRCMismatch = errors.New("CRC mismatch")

	// ErrUnsupportedMagic indicates an unsupported protocol version
	ErrUnsupportedMagic = errors.New("unsupported magic byte")

	// ErrCompressionFailed indicates compression failed
	ErrCompressionFailed = errors.New("compression failed")

	// ErrDecompressionFailed indicates decompression failed
	ErrDecompressionFailed = errors.New("decompression failed")
)