use serde::{Serialize, Deserialize};

mod maelstrom;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Message {
    // Request
    Init {
        node_id: String,
        node_ids: Vec<String>
    },
    Add {
        delta: i32,
    },
    Read { },

    // Response
    InitOk { },
    AddOk { },
    ReadOk {
        value: i32,
    },

    // Internal
    Error {
        code: i32,
        text: String,
    }
}

fn main() {
    let node = maelstrom::Node::new();

    let mut my_node_id: String = "0".to_string();
    let mut my_node_ids: Vec<String> = vec![];
    let mut counter: i32 = 0;

    node.run::<Message, _, _>(|message| async move {
        match message.body.rest {
            Message::Init { ref node_id, ref node_ids } => {
                my_node_id = node_id.to_string();
                my_node_ids = node_ids.to_vec();
                message.reply(Message::InitOk { });
            },
            Message::Add { delta } => {
                counter += delta;
                message.reply(Message::AddOk { });
            },
            Message::Read {  } => {
                message.reply(Message::ReadOk { value: counter });
            },
            Message::Error { code, text } => {
                println!("[{code}]: {text}");
            },
            _ => (),
        }
    });
}

