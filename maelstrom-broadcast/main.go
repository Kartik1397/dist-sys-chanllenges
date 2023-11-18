package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"
    "fmt"

	"github.com/emirpasic/gods/trees/btree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxRetry = 100

func main() {
    n := maelstrom.NewNode()
    br := newBroadcaster(n, 10)
    defer br.close()
    s := &server{n: n, nodeID: n.ID(), ids: make(map[int]struct{}), br: br}

    n.Handle("init", s.initHandler)
    n.Handle("topology", s.topologyHandler)
    n.Handle("read", s.readHandler)
    n.Handle("broadcast", s.broadcastHandler)

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

type server struct {
    n *maelstrom.Node
    nodeID string
    id int
    br     *broadcaster

    idsMu sync.RWMutex
    ids map[int]struct{}

    nodesMu sync.RWMutex
    tree *btree.Tree
}

func (s *server) initHandler(msg maelstrom.Message) error {
    s.nodeID = s.n.ID()
    id, err := strconv.Atoi(s.nodeID[1:])
    if err != nil {
        return err
    }
    s.id = id
    return nil
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
    tree := btree.NewWithIntComparator(len(s.n.NodeIDs()))
    for i := 0; i < len(s.n.NodeIDs()); i++ {
        tree.Put(i, fmt.Sprintf("n%d", i))
    }

    s.nodesMu.Lock()
    s.tree = tree
    s.nodesMu.Unlock()

    return s.n.Reply(msg, map[string]any{
        "type": "topology_ok",
    })
}

func (s *server) readHandler(msg maelstrom.Message) error {
    ids := s.getAllIDs()

    return s.n.Reply(msg, map[string]any{
        "type": "read_ok",
        "messages": ids,
    })
}

func (s *server) getAllIDs() []int {
    s.idsMu.RLock()
    ids := make([]int, 0, len(s.ids))
    for id := range s.ids {
        ids = append(ids, id)
    }
    s.idsMu.RUnlock()

    return ids
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    go func() {
        _ = s.n.Reply(msg, map[string]any{
            "type": "broadcast_ok",
        })
    }()

    id := int(body["message"].(float64))
    s.idsMu.Lock()
    if _, exists := s.ids[id]; exists {
        s.idsMu.Unlock()
        return nil
    }
    s.ids[id] = struct{}{}
    s.idsMu.Unlock()

    return s.broadcast(msg.Src, body);
}


func (s *server) broadcast(src string, body map[string]any) error {
    s.nodesMu.RLock()
    n := s.tree.GetNode(s.id)
    defer s.nodesMu.RUnlock()

    var neighbours []string
    if n.Parent != nil {
        neighbours = append(neighbours, n.Parent.Entries[0].Value.(string))
    }

    for _, child := range n.Children {
        for _, entry := range child.Entries {
            neighbours = append(neighbours, entry.Value.(string))
        }
    }

    for _, dst := range neighbours {
        if dst == src || dst == s.nodeID {
            continue
        }

        s.br.broadcast(broadcastMsg{
            dst: dst,
            body: body,
        })
    }

    return nil
}

type broadcastMsg struct {
    dst string
    body map[string]any
}

type broadcaster struct {
    cancel context.CancelFunc
    ch     chan broadcastMsg
}

func newBroadcaster(n *maelstrom.Node, worker int) *broadcaster {
    ch := make(chan broadcastMsg)
    ctx, cancel := context.WithCancel(context.Background())

    for i := 0; i < worker; i++ {
        go func() {
            for {
                select {
                case msg := <-ch:
                    for {
                        if err := n.Send(msg.dst, msg.body); err != nil {
                            time.Sleep(time.Millisecond * 2)
                            continue
                        }
                        break
                    }
                case <-ctx.Done():
                    return
                }
            }
        }()
    }

    return &broadcaster{
        ch:     ch,
        cancel: cancel,
    }
}

func (b *broadcaster) broadcast(msg broadcastMsg) {
    b.ch <- msg
}

func (b *broadcaster) close() {
    b.cancel()
}

