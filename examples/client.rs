

#[tokio::main]
async fn main() {
    let client = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
    // let x = client.request(reqwest::Method::GET, "http://192.168.179.133:8080/post").send().await.unwrap();
    // println!("x = {:?}", x);
    let x = client.request(reqwest::Method::GET, "http://nghttp2.org/post").send().await.unwrap();
    println!("x = {:?}", x);
    // let body = reqwest::get("https://www.rust-lang.org")
    // .await.unwrap()
    // .text()
    // .await.unwrap();

    // println!("body = {:?}", body);
}