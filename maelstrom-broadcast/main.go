package main

import (
    "encoding/json"
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
    messages := []any{}

    n := maelstrom.NewNode()

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        body["type"] = "broadcast_ok"
        messages = append(messages, body["message"])
        delete(body, "message")
        return n.Reply(msg, body)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        body["type"] = "read_ok"
        body["messages"] = messages
        return n.Reply(msg, body)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        body["type"] = "topology_ok"
        delete(body, "topology")
        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

