package file

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"time"

	msg "github.com/zerofox-oss/go-msg"
)

type Server struct {
	// Directory that contains the files to process
	DirName string

	// DeleteAfter informs whether to remove the files from the directory
	// after thy have been processed successfully.
	DeleteAfter bool

	// maxConcurrentReceives is a buffered channel which acts as
	// a shared lock that limits the number of concurrent goroutines
	maxConcurrentReceives chan struct{}

	listenerCtx        context.Context
	listenerCancelFunc context.CancelFunc

	receiverCtx        context.Context
	receiverCancelFunc context.CancelFunc
}

// Ensure that Server implements msg.Server
var _ msg.Server = &Server{}

func (s *Server) Serve(r msg.Receiver) error {

	for {
		select {
		case <-s.listenerCtx.Done():
			// do something w/ File
			return msg.ErrServerClosed
		default:
			files, err := ioutil.ReadDir(s.DirName)
			if err != nil {
				return err
			}

			for _, fileInfo := range files {
				// Take a slot from the buffered channel
				log.Println("[DEBUG] Ranging over files")
				s.maxConcurrentReceives <- struct{}{}

				f, err := os.Open(fileInfo.Name())
				if err != nil {
					log.Printf("[ERROR] Unable to open file %s; err: %s", fileInfo.Name(), err)
					continue
				}
				go func(f *os.File) {
					defer func() {
						<-s.maxConcurrentReceives
					}()

					m := &msg.Message{
						Attributes: nil,
						Body:       f,
					}

					rErr := r.Receive(s.receiverCtx, m)
					if rErr != nil {
						log.Printf("[ERROR] Could not process file %s; err: %s", f.Name(), rErr)
					}

					// Delete if configured and processed succesfully
					if s.DeleteAfter && err == nil {
						os.Remove(f.Name())
					}
				}(f)
			}
		}
	}
}

// shutdownPollInterval is how often we poll for quiescence
// during Server.Shutdown.
const shutdownPollInterval = 50 * time.Millisecond

// Shutdown attempts to gracefully shut down the Server without
// interrupting any messages currently being processed by a Receiver.
// When Shutdown is signalled, the Server stops polling for new Messages
// and then it waits for all of the active goroutines to complete.
//
// If the provided context expires before the shutdown is complete,
// then any remaining goroutines will be killed and the context's error
// is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		panic("invalid context (nil)")
	}
	s.listenerCancelFunc()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.receiverCancelFunc()
			return ctx.Err()

		case <-ticker.C:
			if len(s.maxConcurrentReceives) == 0 {
				return msg.ErrServerClosed
			}
		}
	}
}

// NewServer creates and initialized a new Server.
func NewServer(dirName string, deleteAfter bool, cc int) *Server {
	listenerCtx, listenerCancelFunc := context.WithCancel(context.Background())
	receiverCtx, receiverCancelFunc := context.WithCancel(context.Background())

	srv := &Server{
		DirName:     dirName,
		DeleteAfter: deleteAfter,

		listenerCtx:           listenerCtx,
		listenerCancelFunc:    listenerCancelFunc,
		receiverCtx:           receiverCtx,
		receiverCancelFunc:    receiverCancelFunc,
		maxConcurrentReceives: make(chan struct{}, cc),
	}

	return srv
}
