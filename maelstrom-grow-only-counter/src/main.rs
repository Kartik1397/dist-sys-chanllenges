use serde::{Serialize, Deserialize};
use std::io::{self, BufRead, Write};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum RequestBody {
    Init {
        node_id: String,
        node_ids: Vec<String>,
        msg_id: i32,
    },
    Add {
        delta: i32,
        msg_id: i32,
    },
    Read {
        msg_id: i32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum ResponseBody {
    InitOk {
        in_reply_to: i32,
    },
    AddOk {
        in_reply_to: i32,
    },
    ReadOk {
        value: i32,
        in_reply_to: i32,
    },
    Error {
        in_reply_to: i32,
        code: i32,
        text: String,
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RequestMessage {
    src: String,
    dest: String,
    body: RequestBody,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}

struct Node {
    id: String,
    node_ids: Vec<String>,
    counter: i32,
}

impl RequestMessage {
    fn reply(&self, resp_body: ResponseBody) {
        let mut stdout = io::stdout();

        let response: ResponseMessage = ResponseMessage {
            src: self.dest.to_string(),
            dest: self.src.to_string(),
            body: resp_body,
        };

        let response_string = serde_json::to_string(&response).unwrap();

        writeln!(stdout, "{}", response_string).expect("Uanble to write to stdout");
        stdout.flush().expect("Unable to flush stdout");
    }
}

impl Node {
    fn send(&self, dest: String, body: RequestBody) {
        let mut stdout = io::stdout();

        let response: RequestMessage = RequestMessage {
            src: self.id.to_string(),
            dest,
            body,
        };

        let response_string = serde_json::to_string(&response).unwrap();

        writeln!(stdout, "{}", response_string).expect("Uanble to write to stdout");
        stdout.flush().expect("Unable to flush stdout");
    }
}

fn main() {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    let mut node: Node = Node {
        id: "0".to_string(),
        node_ids: vec![],
        counter: 0,
    };


    while let Some(line) = lines.next() {
        match line {
            Ok(line) => {
                let deserialized: RequestMessage = serde_json::from_str(&line).unwrap();
                let deserialized_clone = deserialized.clone();

                match deserialized.body {
                    RequestBody::Init { msg_id, node_id, node_ids } => {
                        node.id = node_id;
                        node.node_ids = node_ids;

                        deserialized_clone.reply(ResponseBody::InitOk {
                            in_reply_to: msg_id
                        });
                    },
                    RequestBody::Add { delta, msg_id } => {
                        node.counter += delta;
                        deserialized_clone.reply(ResponseBody::AddOk {
                            in_reply_to: msg_id
                        });
                    },
                    RequestBody::Read { msg_id } => {
                        deserialized_clone.reply(ResponseBody::ReadOk {
                            value: node.counter,
                            in_reply_to: msg_id
                        });
                    },
                }
            },
            Err(_error) => {
                println!("Error while reading line from stdin");
            }
        }
    }
}

