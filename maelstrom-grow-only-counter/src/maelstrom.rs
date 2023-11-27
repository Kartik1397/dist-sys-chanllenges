use serde::{Serialize, Deserialize};
use std::io::{self, BufRead, Write};
use async_std::task::spawn;
use futures::Future;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Body<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<i32>,

    #[serde(flatten)]
    pub rest: T,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Message<T> {
    src: String,
    dest: String,
    pub body: Body<T>,
}

pub struct Node {
    id: Option<String>,
    node_ids: Vec<String>,
}

impl<T> Message<T>
where T: Serialize {
    pub fn reply(&self, resp: T) {
        let mut stdout = io::stdout();

        let response = Message::<T> {
            src: self.dest.to_string(),
            dest: self.src.to_string(),
            body: Body::<T> {
                msg_id: None,
                in_reply_to: self.body.msg_id,
                rest: resp
            },
        };

        let response_string = serde_json::to_string(&response).unwrap();

        writeln!(stdout, "{}", response_string).expect("Uanble to write to stdout");
        stdout.flush().expect("Unable to flush stdout");
    }
}

impl Node
{
    pub fn new() -> Node {
        let node = Node {
            id: None,
            node_ids: vec![],
        };
        return node;
    }

    pub fn run<T, Fut, F>(&self, mut handler: F)
    where 
        F: FnMut(Message<T>) -> Fut,
        Fut: Future<Output = ()>, // + Send + 'static,
        T: for<'a> Deserialize<'a> + std::fmt::Debug
    {
        let stdin = io::stdin();
        let mut lines = stdin.lock().lines();

        while let Some(line) = lines.next() {
            match line {
                Ok(line) => {
                     let deserialized: Message<T> = serde_json::from_str(&line).unwrap();
                     handler(deserialized);
                },
                Err(_error) => {
                    println!("Error while reading line from stdin");
                }
            }
        }
    }

    pub fn get_id(&self) -> Option<String> {
        return self.id.clone();
    }

    pub fn get_node_ids(&self) -> Vec<String> {
        return self.node_ids.clone();
    }

//    pub fn set_id(&mut self, id: Option<String>) {
//        self.id = id;
//    }
//
//    pub fn set_node_ids(&mut self, node_ids: &Vec<String>) {
//        self.node_ids = node_ids.to_vec();
//    }
}
