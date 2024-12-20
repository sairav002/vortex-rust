
use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message<Payload> {
    src: String,
    dest: String,
    body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body<Payload> {
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum RequestPayload {
    Echo { echo: String },
    Init { node_id: String, node_ids: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum ResponsePayload {
    EchoOk { echo: String },
    InitOk,
}


struct EchoNode {
    id: u64,
}

impl EchoNode {
    fn handle(
        &mut self, 
        input: Message<RequestPayload>, 
        output: &mut StdoutLock
    ) -> anyhow::Result<()> {
        match input.body.payload {
            RequestPayload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: ResponsePayload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to echo")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
            RequestPayload::Init { .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: ResponsePayload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.id += 1;
            }
        }
        Ok(())
    }
}


fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let input_stream = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<RequestPayload>>();
    
    let mut stdout = std::io::stdout().lock();
    
    let mut node = EchoNode { id: 0 };

    for input in input_stream {
        let input = input.context("Malestrom input from STDIN could not be deserialized")?;
        node
            .handle(input, &mut stdout)
            .context("Node step function failed")?;
    }

    Ok(())
}
