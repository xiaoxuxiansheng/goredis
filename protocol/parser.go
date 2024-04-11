package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"github.com/xiaoxuxiansheng/goredis/handler"
	"github.com/xiaoxuxiansheng/goredis/lib/pool"
	"github.com/xiaoxuxiansheng/goredis/log"
)

type lineParser func(header []byte, reader *bufio.Reader) *handler.Droplet

type Parser struct {
	lineParsers map[byte]lineParser
	logger      log.Logger
}

func NewParser(logger log.Logger) handler.Parser {
	p := Parser{
		logger: logger,
	}
	p.lineParsers = map[byte]lineParser{
		'+': p.parseSimpleString,
		'-': p.parseError,
		':': p.parseInt,
		'$': p.parseBulk,
		'*': p.parseMultiBulk,
	}
	return &p
}

func (p *Parser) ParseStream(reader io.Reader) <-chan *handler.Droplet {
	ch := make(chan *handler.Droplet)
	pool.Submit(
		func() {
			p.parse(reader, ch)
		})
	return ch
}

func (p *Parser) parse(rawReader io.Reader, ch chan<- *handler.Droplet) {
	reader := bufio.NewReader(rawReader)
	for {
		firstLine, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &handler.Droplet{
				Reply: handler.NewErrReply(err.Error()),
				Err:   err,
			}
			return
		}

		length := len(firstLine)
		if length <= 2 || firstLine[length-1] != '\n' || firstLine[length-2] != '\r' {
			continue
		}

		firstLine = bytes.TrimSuffix(firstLine, []byte{'\r', '\n'})
		lineParseFunc, ok := p.lineParsers[firstLine[0]]
		if !ok {
			p.logger.Errorf("[parser] invalid line handler: %s", firstLine[0])
			continue
		}

		ch <- lineParseFunc(firstLine, reader)
	}
}

// 解析简单 string 类型
func (p *Parser) parseSimpleString(header []byte, reader *bufio.Reader) *handler.Droplet {
	content := header[1:]
	return &handler.Droplet{
		Reply: handler.NewSimpleStringReply(string(content)),
	}
}

// 解析简单 int 类型
func (p *Parser) parseInt(header []byte, reader *bufio.Reader) *handler.Droplet {

	i, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return &handler.Droplet{
			Err:   err,
			Reply: handler.NewErrReply(err.Error()),
		}
	}

	return &handler.Droplet{
		Reply: handler.NewIntReply(i),
	}
}

// 解析错误类型
func (p *Parser) parseError(header []byte, reader *bufio.Reader) *handler.Droplet {
	return &handler.Droplet{
		Reply: handler.NewErrReply(string(header[1:])),
	}
}

// 解析定长 string 类型
func (p *Parser) parseBulk(header []byte, reader *bufio.Reader) *handler.Droplet {
	// 解析定长 string
	body, err := p.parseBulkBody(header, reader)
	if err != nil {
		return &handler.Droplet{
			Reply: handler.NewErrReply(err.Error()),
			Err:   err,
		}
	}
	return &handler.Droplet{
		Reply: handler.NewBulkReply(body),
	}
}

// 解析定长 string
func (p *Parser) parseBulkBody(header []byte, reader *bufio.Reader) ([]byte, error) {
	// 获取 string 长度
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	// 长度 + 2，把 CRLF 也考虑在内
	body := make([]byte, strLen+2)
	// 从 reader 中读取对应长度
	if _, err = io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	return body[:len(body)-2], nil
}

// 解析
func (p *Parser) parseMultiBulk(header []byte, reader *bufio.Reader) (droplet *handler.Droplet) {
	var _err error
	defer func() {
		if _err != nil {
			droplet = &handler.Droplet{
				Reply: handler.NewErrReply(_err.Error()),
				Err:   _err,
			}
		}
	}()

	// 获取数组长度
	length, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		_err = err
		return
	}

	if length <= 0 {
		return &handler.Droplet{
			Reply: handler.NewEmptyMultiBulkReply(),
		}
	}

	lines := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		// 获取每个 bulk 首行
		firstLine, err := reader.ReadBytes('\n')
		if err != nil {
			_err = err
			return
		}

		// bulk 首行格式校验
		length := len(firstLine)
		if length < 4 || firstLine[length-2] != '\r' || firstLine[length-1] != '\n' || firstLine[0] != '$' {
			continue
		}

		// bulk 解析
		bulkBody, err := p.parseBulkBody(firstLine[:length-2], reader)
		if err != nil {
			_err = err
			return
		}

		lines = append(lines, bulkBody)
	}

	return &handler.Droplet{
		Reply: handler.NewMultiBulkReply(lines),
	}
}
