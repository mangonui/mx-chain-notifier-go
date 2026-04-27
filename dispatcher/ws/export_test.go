package ws

// ArgsWSDispatcher -
type ArgsWSDispatcher struct {
	argsWebSocketDispatcher
}

// NewTestWSDispatcher -
func NewTestWSDispatcher(args ArgsWSDispatcher) (*websocketDispatcher, error) {
	wsArgs := argsWebSocketDispatcher{
		Dispatcher: args.Dispatcher,
		Conn:       args.Conn,
		Marshaller: args.Marshaller,
	}

	return newWebSocketDispatcher(wsArgs)
}

// WritePump -
func (wd *websocketDispatcher) WritePump() {
	wd.writePump()
}

// ReadPump -
func (wd *websocketDispatcher) ReadPump() {
	wd.readPump()
}

// ReadSendChannel -
func (wd *websocketDispatcher) ReadSendChannel() []byte {
	d := <-wd.send
	return d
}

func (wd *websocketDispatcher) SendQueueLen() int {
	return len(wd.send)
}

func (wd *websocketDispatcher) SendQueueCap() int {
	return cap(wd.send)
}
