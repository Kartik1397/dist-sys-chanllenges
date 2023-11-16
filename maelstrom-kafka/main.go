package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    s := &server{
        n: n,
        nodeID: n.ID(),
        logger: logger{
            log: make(map[string][][]int),
            offsetMap: make(map[int]int),
            offset: 0,
            commitedOffset: make(map[string]int),
        },
    }

    n.Handle("topology", s.topologyHandler)
    n.Handle("send", s.sendHandler)
    n.Handle("poll", s.pollHandler)
    n.Handle("commit_offsets", s.commitOffsetsHandler)
    n.Handle("list_committed_offsets", s.listCommitedOffsetsHandler)

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

type server struct {
    n *maelstrom.Node
    nodeID string
    logger logger

    mux sync.RWMutex
}

type sendMsg struct {
    Type string `json:"type"`
    Key string `json:"key"`
    Msg int `json:"msg"`
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
    return s.n.Reply(msg, map[string]any{
        "type": "topology_ok",
    })
}

func (s *server) sendHandler(msg maelstrom.Message) error {
    var body sendMsg
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    return s.n.Reply(msg, map[string]any{
        "type": "send_ok",
        "offset": s.logger.append(body.Key, body.Msg),
    })
}

type pollMsg struct {
    Type string `json:"type"`
    Offsets map[string]int `json:"offsets"`
}

func (s *server) pollHandler(msg maelstrom.Message) error {
    var body pollMsg
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    s.mux.Lock()
    msgs := make(map[string][][]int)
    for key, offset := range body.Offsets {
        msgs[key] = s.logger.poll(key, offset)
    }
    s.mux.Unlock()

    return s.n.Reply(msg, map[string]any{
        "type": "poll_ok",
        "msgs": msgs,
    })
}

type commitOffsetsMsg struct {
    Type string `json:"type"`
    Offsets map[string]int `json:"offsets"`
}

func (s *server) commitOffsetsHandler(msg maelstrom.Message) error {
    var body commitOffsetsMsg
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    s.mux.Lock()
    for key, offset := range body.Offsets {
        s.logger.commitOffset(key, offset)
    }
    s.mux.Unlock()

    return s.n.Reply(msg, map[string]any{
        "type": "commit_offsets_ok",
    })
}

type listCommitedOffsetsMsg struct {
    Type string `json:"type"`
    Keys []string `json:"keys"`
}

func (s *server) listCommitedOffsetsHandler(msg maelstrom.Message) error {
    var body listCommitedOffsetsMsg
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    s.mux.Lock()
    offsets := make(map[string]int)
    for _, key := range body.Keys {
        offsets[key] = s.logger.getCommitedOffset(key)
    }
    s.mux.Unlock()

    return s.n.Reply(msg, map[string]any{
        "type": "list_committed_offsets_ok",
        "offsets": offsets,
    })
}

type logger struct {
    log map[string][][]int
    offsetMap map[int]int
    offset int
    commitedOffset map[string]int

    mux sync.RWMutex
}

func (l *logger) append(key string, payload int) int {
    l.mux.Lock()
    if _, ok := l.log[key]; !ok {
        l.log[key] = [][]int{}
    }

    l.offset += 1

    l.log[key] = append(l.log[key], []int{l.offset, payload})
    l.offsetMap[l.offset] = len(l.log[key]) - 1

    offset := l.offset
    l.mux.Unlock()

    return offset
}

func (l *logger) poll(key string, offset int) [][]int {
    l.mux.Lock()
    ans := [][]int{}
    if val, ok := l.log[key]; ok {
        startIdx := l.offsetMap[offset]
        for i := startIdx; i < len(val); i++ {
            ans = append(ans, l.log[key][i])
        }
    }
    l.mux.Unlock()
    return ans
}

func (l *logger) commitOffset(key string, offset int) {
    l.mux.Lock()
    l.commitedOffset[key] = offset
    l.mux.Unlock()
}

func (l *logger) getCommitedOffset(key string) int {
    l.mux.Lock()
    ans := l.commitedOffset[key]
    l.mux.Unlock()
    return ans
}

