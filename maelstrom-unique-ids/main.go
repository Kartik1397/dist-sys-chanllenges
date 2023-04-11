package main

import (
    "encoding/json"
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
    uuid "github.com/google/uuid"
)


func main() {
    n := maelstrom.NewNode()
    n.Handle("generate", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        body["type"] = "generate_ok"
        body["id"] = uuid.New().String()
        return n.Reply(msg, body)
    })
    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}

