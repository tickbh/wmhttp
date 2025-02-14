#![warn(rust_2018_idioms)]

use algorithm::buf::BinaryMut;
use async_trait::async_trait;
use flate2::GzBuilder;
use webparse::{HeaderName, Response};
use std::{
    env,
    error::Error,
    io::{self},
    net::SocketAddr,
};
use tokio::{
    fs::File,
    net::{TcpListener, TcpStream},
};

use wmhttp::{self, Body, HttpTrait, ProtResult, RecvRequest, RecvResponse, Server};

struct Operate;

#[async_trait]
impl HttpTrait for Operate {
    async fn operate(&mut self, mut req: RecvRequest) -> ProtResult<RecvResponse> {
        let mut builder = Response::builder().version(req.version().clone());
        let body = match &*req.url().path {
            "/plaintext" | "/" => {
                builder = builder.header("content-type", "text/plain; charset=utf-8");
                Body::new_text("Hello, World!".to_string())
            }
            "/post" => {
                let body = req.body_mut();
                // let mut buf = [0u8; 10];
                // if let Ok(len) = body.read(&mut buf).await {
                //     println!("skip = {:?}", &buf[..len]);
                // }
                let mut binary = BinaryMut::new();
                body.read_all(&mut binary).await.unwrap();
                println!("binary = {:?}", binary);

                builder = builder.header("content-type", "text/plain");
                Body::new_binary(binary)
                // format!("Hello, World! {:?}", TryInto::<String>::try_into(binary)).to_string()
            }
            "/json" => {
                builder = builder.header("content-type", "application/json");
                #[derive(Serialize)]
                struct Message {
                    message: &'static str,
                }
                Body::new_text(
                    serde_json::to_string(&Message {
                        message: "Hello, World!",
                    })
                    .unwrap(),
                )
            }
            "/file" => {
                builder = builder.header("content-type", "application/json");
                let file = File::open("README.md").await?;
                let length = file.metadata().await?.len();
                Body::new_file(file, length)
            }
            _ => {
                builder = builder.status(404);
                Body::empty()
            }
        };

        let response = builder
            // .header("Content-Length", length as usize)
            .header(HeaderName::TRANSFER_ENCODING, "chunked")
            .body(body)
            .map_err(|_err| io::Error::new(io::ErrorKind::Other, ""))?;
        Ok(response)
    }
}

async fn process(stream: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    let mut server = Server::new(stream, Some(addr));
    server.set_callback_http(Box::new(Operate));
    let _ret = server.incoming().await;
    Ok(())
}

#[tokio::main]
async fn main() -> ProtResult<()> {
    env_logger::init();
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let server = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);
    let _gz = GzBuilder::new();
    loop {
        let (stream, addr) = server.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = process(stream, addr).await {
                println!("failed to process connection; error = {}", e);
            }
        });
    }
}
