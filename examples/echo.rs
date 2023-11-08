use std::{env, error::Error, time::Duration};
use serde::ser;
use tokio::{net::TcpListener};
use webparse::{Request, Response};
use wenmeng::{self, ProtResult, RecvStream, Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let server = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);
    loop {
        let (stream, addr) = server.accept().await?;
        tokio::spawn(async move {
            let mut server = Server::new(stream, Some(addr));
            // server.set_read_timeout(Some(Duration::new(0, 100)));
            // server.set_write_timeout(Some(Duration::new(0, 100)));
            // server.set_timeout(Some(Duration::new(0, 100)));
            async fn operate(req: Request<RecvStream>) -> ProtResult<Response<String>> {
                tokio::time::sleep(Duration::new(1, 1)).await;
                let response = Response::builder()
                    .version(req.version().clone())
                    .body("Hello World\r\n".to_string())?;
                Ok(response)
            }
            let e = server.incoming(operate).await;
            println!("close server ==== addr = {:?} e = {:?}", addr, e);
        });
    }
}
