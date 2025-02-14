// Copyright 2022 - 2023 Wenmeng See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//
// Author: tickbh
// -----
// Created Date: 2023/10/07 09:41:02

use std::io;

use std::sync::Arc;
use std::time::Duration;

use crate::http2::{self, ClientH2Connection};
use crate::ws::{ClientWsConnection, WsHandshake, WsOption, WsTrait};
use crate::{http1::ClientH1Connection, ProtError};
use crate::{
    Body, MaybeHttpsStream, Middleware, ProtResult, RecvRequest, RecvResponse, TimeoutLayer,
};
use algorithm::buf::Binary;
use base64::prelude::*;
use futures::StreamExt;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
use webparse::http2::frame::Settings;
use webparse::http2::{DEFAULT_INITIAL_WINDOW_SIZE, DEFAULT_MAX_FRAME_SIZE, HTTP2_MAGIC};
use webparse::{ws::OwnedMessage, Request, Url, WebError};

use super::middle::BaseMiddleware;
use super::proxy::ProxyScheme;

pub struct Builder {
    inner: ClientOption,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner: ClientOption::default(),
        }
    }

    pub fn http2_only(mut self, http2_only: bool) -> Self {
        self.inner.http2_only = http2_only;
        self
    }

    pub fn http2(mut self, http2: bool) -> Self {
        self.inner.http2 = http2;
        self
    }

    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        if self.inner.timeout.is_none() {
            self.inner.timeout = Some(TimeoutLayer::new());
        }
        self.inner.timeout.as_mut().unwrap().connect_timeout = Some(connect_timeout);
        self
    }

    pub fn ka_timeout(mut self, ka_timeout: Duration) -> Self {
        if self.inner.timeout.is_none() {
            self.inner.timeout = Some(TimeoutLayer::new());
        }
        self.inner.timeout.as_mut().unwrap().ka_timeout = Some(ka_timeout);
        self
    }

    pub fn read_timeout(mut self, read_timeout: Duration) -> Self {
        if self.inner.timeout.is_none() {
            self.inner.timeout = Some(TimeoutLayer::new());
        }
        self.inner.timeout.as_mut().unwrap().read_timeout = Some(read_timeout);
        self
    }

    pub fn write_timeout(mut self, write_timeout: Duration) -> Self {
        if self.inner.timeout.is_none() {
            self.inner.timeout = Some(TimeoutLayer::new());
        }
        self.inner.timeout.as_mut().unwrap().write_timeout = Some(write_timeout);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        if self.inner.timeout.is_none() {
            self.inner.timeout = Some(TimeoutLayer::new());
        }
        self.inner.timeout.as_mut().unwrap().timeout = Some(timeout);
        self
    }

    pub fn timeout_layer(mut self, timeout: Option<TimeoutLayer>) -> Self {
        self.inner.timeout = timeout;
        self
    }

    pub fn add_proxy(mut self, val: &str) -> ProtResult<Self> {
        let proxy = ProxyScheme::try_from(val)?;
        self.inner.proxies.push(proxy);
        Ok(self)
    }

    pub fn url<U>(mut self, url: U) -> ProtResult<Self>
    where
        Url: TryFrom<U>,
        <Url as TryFrom<U>>::Error: Into<WebError>,
    {
        let url = TryInto::<Url>::try_into(url)
            .map_err(|_e| ProtError::Extension("unknown connection url"))?;

        self.inner.url = Some(url);
        Ok(self)
    }

    pub fn value(self) -> ClientOption {
        self.inner
    }

    pub fn middle<M: Middleware + 'static>(mut self, middle: M) -> Self {
        self.inner.middles.push(Box::new(middle));
        self
    }

    pub fn option(mut self, option: ClientOption) -> Self {
        self.inner = option;
        self
    }

    pub async fn connect_by_stream(self, stream: TcpStream) -> ProtResult<Client> {
        Ok(Client::new(self.inner, MaybeHttpsStream::Http(stream)))
    }

    async fn inner_connect<A: ToSocketAddrs>(&self, addr: A) -> ProtResult<TcpStream> {
        if self.inner.timeout.is_some() {
            // 获取是否配置了连接超时, 如果有连接超时那么指定timeout
            if let Some(connect) = &self.inner.timeout.as_ref().unwrap().connect_timeout {
                match tokio::time::timeout(*connect, TcpStream::connect(addr)).await {
                    Ok(v) => return Ok(v?),
                    Err(_) => return Err(ProtError::connect_timeout("client")),
                }
            }
        }
        let tcp = TcpStream::connect(addr).await?;
        Ok(tcp)
    }

    pub async fn connect(self) -> ProtResult<Client> {
        self.connect_with_domain("").await
    }

    pub async fn connect_with_domain(self, domain: &str) -> ProtResult<Client> {
        if self.inner.url.is_none() {
            return Err(ProtError::Extension("unknown connection url"));
        }
        let url = self.inner.url.as_ref().unwrap();
        if self.inner.proxies.len() > 0 {
            for p in self.inner.proxies.iter() {
                match p.connect(&url).await? {
                    Some(tcp) => {
                        if url.scheme.is_https() {
                            return self.connect_tls_by_stream_with_domain(tcp, domain).await;
                        } else {
                            let proxy = p.clone();
                            let mut client = Client::new(self.inner, MaybeHttpsStream::Http(tcp));
                            client.set_proxy(proxy);
                            return Ok(client);
                        }
                    }
                    None => continue,
                }
            }
            return Err(ProtError::Extension("not proxy error!"));
        } else {
            if !ProxyScheme::is_no_proxy(url.domain.as_ref().unwrap_or(&String::new())) {
                let proxies = ProxyScheme::get_env_proxies();
                for p in proxies.iter() {
                    match p.connect(&url).await? {
                        Some(tcp) => {
                            if url.scheme.is_https() {
                                return self.connect_tls_by_stream_with_domain(tcp, domain).await;
                            } else {
                                let proxy = p.clone();
                                let mut client =
                                    Client::new(self.inner, MaybeHttpsStream::Http(tcp));
                                client.set_proxy(proxy);
                                return Ok(client);
                            }
                        }
                        None => continue,
                    }
                }
            }
            if url.scheme.is_https() {
                let connect = url.get_connect_url();
                let stream = self.inner_connect(&connect.unwrap()).await?;
                self.connect_tls_by_stream_with_domain(stream, domain).await
            } else {
                let tcp = self.inner_connect(url.get_connect_url().unwrap()).await?;
                Ok(Client::new(self.inner, MaybeHttpsStream::Http(tcp)))
            }
        }
    }

    pub async fn connect_tls_by_stream(self, stream: TcpStream) -> ProtResult<Client> {
        self.connect_tls_by_stream_with_domain(stream, "").await
    }

    pub async fn connect_tls_by_stream_with_domain(
        mut self,
        stream: TcpStream,
        domain: &str,
    ) -> ProtResult<Client> {
        if self.inner.url.is_none() {
            return Err(ProtError::Extension("unknown connection url"));
        }
        let url = self.inner.url.as_ref().unwrap();
        let connect = url.get_connect_url();
        let name = if domain.len() > 0 {
            domain.to_string()
        } else {
            if url.domain.is_none() || connect.is_none() {
                return Err(ProtError::Extension("unknown connection domain"));
            } else {
                url.domain.clone().unwrap()
            }
        };
        let mut roots = RootCertStore::empty();
        roots.extend(
            webpki_roots::TLS_SERVER_ROOTS
                .iter()
                .cloned(),
        );
        
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = self.inner.get_alpn_protocol();
        let tls_client = Arc::new(config);
        let connector = TlsConnector::from(tls_client);

        // 这里的域名只为认证设置
        let domain = rustls::pki_types::ServerName::try_from(name)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?;

        let outbound = connector.connect(domain, stream).await?;
        let aa = outbound.get_ref().1.alpn_protocol();
        if aa == Some(&ClientOption::H2_PROTOCOL) {
            self.inner.http2_only = true;
        } else {
            self.inner.http2 = true;
            self.inner.http2_only = false;
        }
        Ok(Client::new(self.inner, MaybeHttpsStream::Https(outbound)))
    }
}

pub struct ClientOption {
    pub http2_only: bool,
    pub http2: bool,
    pub settings: Settings,
    pub url: Option<Url>,
    pub timeout: Option<TimeoutLayer>,
    pub proxies: Vec<ProxyScheme>,
    pub middles: Vec<Box<dyn Middleware>>,
}

impl ClientOption {
    pub const H2_PROTOCOL: [u8; 2] = [104, 50];
    pub fn get_alpn_protocol(&self) -> Vec<Vec<u8>> {
        let mut ret = vec![];
        if self.http2_only {
            ret.push(Self::H2_PROTOCOL.to_vec());
        } else {
            ret.push("http/1.1".as_bytes().to_vec());
            if self.http2 {
                ret.push(Self::H2_PROTOCOL.to_vec());
            }
        }
        ret
    }

    pub fn get_http2_setting(&self) -> String {
        self.settings.encode_http_settings()
    }

    pub fn is_ws(&self) -> bool {
        if let Some(url) = &self.url {
            url.scheme.is_ws() || url.scheme.is_wss()
        } else {
            false
        }
    }
}

impl Default for ClientOption {
    fn default() -> Self {
        Self {
            http2_only: false,
            http2: true,
            url: None,
            settings: Default::default(),
            timeout: None,
            proxies: vec![],
            middles: vec![Box::new(BaseMiddleware::new(true))],
        }
    }
}

pub struct Client<T = TcpStream> {
    option: ClientOption,
    sender: Sender<ProtResult<RecvResponse>>,
    receiver: Option<Receiver<ProtResult<RecvResponse>>>,
    req_receiver: Option<Receiver<RecvRequest>>,
    http1: Option<ClientH1Connection<MaybeHttpsStream<T>>>,
    http2: Option<ClientH2Connection<MaybeHttpsStream<T>>>,
    ws: Option<ClientWsConnection<MaybeHttpsStream<T>>>,
    callback_ws: Option<Box<dyn WsTrait>>,
    proxy: Option<ProxyScheme>,
}

impl Client {
    pub fn builder() -> Builder {
        Builder::new()
    }
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static + Send,
{
    pub fn new(option: ClientOption, stream: MaybeHttpsStream<T>) -> Self {
        let (sender, receiver) = channel(10);
        let mut client = Self {
            option,
            sender,
            receiver: Some(receiver),
            req_receiver: None,
            http1: None,
            http2: None,
            ws: None,
            callback_ws: None,
            proxy: None,
        };
        if client.option.http2_only {
            let mut value = http2::Builder::new()
                .initial_window_size(DEFAULT_INITIAL_WINDOW_SIZE)
                .max_concurrent_streams(100)
                .max_frame_size(DEFAULT_MAX_FRAME_SIZE)
                // .set_enable_push(false)
                .client_connection(stream);
            value.set_timeout_layer(client.option.timeout.clone());
            value.set_handshake_status(Binary::from(HTTP2_MAGIC));
            client.http2 = Some(value);
        } else {
            client.http1 = Some(client.build_client_h1_connection(stream));
        }
        client
    }

    fn build_client_h1_connection(
        &self,
        stream: MaybeHttpsStream<T>,
    ) -> ClientH1Connection<MaybeHttpsStream<T>> {
        let mut client = ClientH1Connection::new(stream);
        client.set_timeout_layer(self.option.timeout.clone());
        client
    }

    pub fn set_proxy(&mut self, proxy: ProxyScheme) {
        self.proxy = Some(proxy);
    }

    pub fn set_callback_ws(&mut self, callback_ws: Box<dyn WsTrait>) {
        self.callback_ws = Some(callback_ws);
    }

    pub fn take_callback_ws(&mut self) -> Option<Box<dyn WsTrait>> {
        self.callback_ws.take()
    }

    pub fn into_io(self) -> T {
        if self.http1.is_some() {
            self.http1.unwrap().into_io().into_io()
        } else {
            self.http2.unwrap().into_io().into_io()
        }
    }

    pub fn split(
        &mut self,
    ) -> ProtResult<(Receiver<ProtResult<RecvResponse>>, Sender<RecvRequest>)> {
        if self.receiver.is_none() {
            return Err(ProtError::Extension("receiver error"));
        }
        let (sender, receiver) = channel::<RecvRequest>(10);
        self.req_receiver = Some(receiver);
        Ok((self.receiver.take().unwrap(), sender))
    }

    async fn send_req(&mut self, mut req: RecvRequest) -> ProtResult<()> {
        if let Some(proxy) = &self.proxy {
            proxy.fix_request(&mut req)?;
        }
        for i in 0usize..self.option.middles.len() {
            self.option.middles[i].process_request(&mut req).await?;
        }
        if let Some(h) = &mut self.http1 {
            h.send_request(req)?;
        } else if let Some(h) = &mut self.http2 {
            h.send_request(req)?;
        }
        Ok(())
    }

    pub fn middle<M: Middleware + 'static>(&mut self, middle: M) {
        self.option.middles.push(Box::new(middle));
    }

    pub async fn wait_operate(mut self) -> ProtResult<()> {
        async fn http1_wait<T>(
            connection: &mut Option<ClientH1Connection<T>>,
        ) -> Option<ProtResult<Option<RecvResponse>>>
        where
            T: AsyncRead + AsyncWrite + Unpin,
        {
            if connection.is_some() {
                Some(connection.as_mut().unwrap().incoming().await)
            } else {
                let pend = std::future::pending();
                let () = pend.await;
                None
            }
        }

        async fn http2_wait<T>(
            connection: &mut Option<ClientH2Connection<T>>,
        ) -> Option<ProtResult<Option<RecvResponse>>>
        where
            T: AsyncRead + AsyncWrite + Unpin,
        {
            if connection.is_some() {
                Some(connection.as_mut().unwrap().incoming().await)
            } else {
                let pend = std::future::pending();
                let () = pend.await;
                None
            }
        }

        async fn req_receiver(
            req_receiver: &mut Option<Receiver<RecvRequest>>,
        ) -> Option<RecvRequest> {
            if req_receiver.is_some() {
                req_receiver.as_mut().unwrap().recv().await
            } else {
                let pend = std::future::pending();
                let () = pend.await;
                None
            }
        }
        let (mut ws_receiver, mut ws_option);
        loop {
            let v = tokio::select! {
                r = http1_wait(&mut self.http1) => {
                    r
                }
                r = http2_wait(&mut self.http2) => {
                    r
                }
                req = req_receiver(&mut self.req_receiver) => {
                    if let Some(req) = req {
                        self.send_req(req).await?;
                    } else {
                        self.req_receiver = None;
                    }
                    continue;
                }
                () = self.sender.closed() => {
                    log::trace!("接收方被断开, 此时关闭Client");
                    return Ok(());
                }
            };
            if v.is_none() {
                return Ok(());
            }
            let result = v.unwrap();
            match result {
                Ok(None) => {
                    self.sender
                        .send(Err(ProtError::Extension("close by server")))
                        .await?;
                    return Ok(());
                }
                Err(ProtError::ClientUpgradeHttp2(s)) => {
                    if self.http1.is_some() {
                        self.http2 = Some(self.http1.take().unwrap().into_h2(s));
                        continue;
                    } else {
                        return Err(ProtError::ClientUpgradeHttp2(s));
                    }
                }
                Err(e) => {
                    self.sender.send(Err(e)).await?;
                    return Ok(());
                }
                Ok(Some(r)) => {
                    if r.status() == 101
                        && r.headers().is_contains(&"Connection", "Upgrade".as_bytes())
                    {
                        if r.headers().is_contains(&"Upgrade", "h2c".as_bytes()) {
                            if self.http1.is_some() {
                                self.http2 = Some(
                                    self.http1
                                        .take()
                                        .unwrap()
                                        .into_h2(self.option.settings.clone()),
                                );
                                continue;
                            } else {
                                return Err(ProtError::ClientUpgradeHttp2(
                                    self.option.settings.clone(),
                                ));
                            }
                        } else if r.headers().is_contains(&"Upgrade", "websocket".as_bytes()) {
                            if self.callback_ws.is_none() {
                                return Err(ProtError::Extension("websocket callback is none"));
                            }
                            if self.http1.is_some() {
                                self.ws = Some(self.http1.take().unwrap().into_ws());
                                let (sender, receiver) = channel::<OwnedMessage>(10);
                                let shake = WsHandshake::new(sender, None, r, None);
                                ws_option =
                                    self.callback_ws.as_mut().unwrap().on_open(shake).await?;
                                ws_receiver = receiver;

                                if ws_option.is_some()
                                    && ws_option.as_mut().unwrap().receiver.is_some()
                                {
                                    ws_receiver =
                                        ws_option.as_mut().unwrap().receiver.take().unwrap();
                                }
                                break;
                            } else {
                                return Err(ProtError::ClientUpgradeHttp2(
                                    self.option.settings.clone(),
                                ));
                            }
                        }
                    }
                    self.sender.send(Ok(r)).await?;
                }
            };
        }

        self.inner_oper_ws(ws_receiver, ws_option).await?;

        Ok(())
    }

    async fn inner_oper_ws(
        &mut self,
        mut receiver: Receiver<OwnedMessage>,
        mut option: Option<WsOption>,
    ) -> ProtResult<()> {
        if self.callback_ws.is_none() {
            return Err(ProtError::Extension("unknow callback websocket"));
        }
        loop {
            if let Some(ws) = &mut self.ws {
                tokio::select! {
                    ret = ws.next() => {
                        println!("ws ret = {:?}", ret);
                        match ret {
                            None => {
                                return Ok(());
                            }
                            Some(Ok(msg)) => {
                                match msg {
                                    OwnedMessage::Text(_) | OwnedMessage::Binary(_) => self.callback_ws.as_mut().unwrap().on_message(msg).await?,
                                    OwnedMessage::Close(c) => {
                                        self.callback_ws.as_mut().unwrap().on_close(&c).await;
                                        ws.receiver_close(c)?;
                                    },
                                    OwnedMessage::Ping(v) => {
                                        if let Some(p) = self.callback_ws.as_mut().unwrap().on_ping(v).await? {
                                            ws.send_owned_message(p)?;
                                        }
                                    },
                                    OwnedMessage::Pong(v) => {
                                        self.callback_ws.as_mut().unwrap().on_pong(v).await?;
                                    },
                                }
                            }
                            Some(Err(e)) => return Err(e),
                        }
                    }
                    msg = receiver.recv() => {
                        println!("client msg recv = {:?}", msg);
                        match msg {
                            None => {
                                return Ok(());
                            }
                            Some(msg) => {
                                match &msg {
                                    OwnedMessage::Close(data) => {
                                        ws.receiver_close(data.clone())?;
                                    },
                                    _ => {}
                                }
                                ws.send_owned_message(msg)?;
                            }
                        }
                    }
                    _ = WsOption::interval_wait(&mut option) => {
                        self.callback_ws.as_mut().unwrap().on_interval(&mut option).await?;
                    }
                }
            }
        }
    }

    async fn inner_operate(mut self, req: RecvRequest) -> ProtResult<()> {
        self.send_req(req).await?;
        self.wait_operate().await?;
        Ok(())
    }

    pub async fn wait_ws_operate(self) -> ProtResult<()> {
        if self.option.url.is_none() {
            return Err(ProtError::Extension("unknow url"));
        }
        let mut req = Request::builder()
            .method("GET")
            .url(self.option.url.clone().unwrap())
            .body(Body::empty())
            .unwrap();
        let header = req.headers_mut();
        header.insert("Connection", "Upgrade");
        header.insert("Upgrade", "websocket");
        let key: [u8; 16] = rand::random();
        header.insert("Sec-WebSocket-Key", BASE64_STANDARD.encode(&key));
        header.insert("Sec-WebSocket-Version", "13");
        header.insert("Sec-WebSocket-Protocol", "chat, superchat");
        self.wait_ws_operate_with_req(req).await?;
        Ok(())
    }

    pub async fn wait_ws_operate_with_req(mut self, req: RecvRequest) -> ProtResult<()> {
        if self.option.url.is_none() {
            return Err(ProtError::Extension("unknow url"));
        }
        if self.callback_ws.is_none() {
            return Err(ProtError::Extension("unknow websocket callback"));
        }
        self.send_req(req).await?;
        self.wait_operate().await?;
        Ok(())
    }

    fn rebuild_request(&mut self, req: &mut RecvRequest) {
        // 支持http2且当前为http1尝试升级
        if self.option.http2 {
            if let Some(_) = &self.http1 {
                let header = req.headers_mut();
                header.insert("Connection", "Upgrade, HTTP2-Settings");
                header.insert("Upgrade", "h2c");
                header.insert("HTTP2-Settings", self.option.get_http2_setting());
            }
        }
        // else if self.option.is_ws() {
        //     if let Some(_) = &self.http1 {
        //         let header = req.headers_mut();
        //         header.insert("Connection", "Upgrade");
        //         header.insert("Upgrade", "websocket");
        //         let key: [u8; 16] = rand::random();
        //         header.insert("Sec-WebSocket-Key", base64::encode(&key));
        //         header.insert("Sec-WebSocket-Version", "13");
        //         header.insert("Sec-WebSocket-Protocol", "chat, superchat");
        //     }
        // }
    }

    pub async fn send(
        mut self,
        mut req: RecvRequest,
    ) -> ProtResult<Receiver<ProtResult<RecvResponse>>> {
        self.rebuild_request(&mut req);
        let (r, s) = self.split()?;
        tokio::spawn(async move {
            let _sender = s;
            if let Err(e) = self.inner_operate(req).await {
                println!("http数据请求时发生错误: {:?}", e);
            }
        });
        Ok(r)
    }

    pub async fn send2(
        mut self,
        mut req: RecvRequest,
    ) -> ProtResult<(Receiver<ProtResult<RecvResponse>>, Sender<RecvRequest>)> {
        self.rebuild_request(&mut req);
        let (r, s) = self.split()?;
        tokio::spawn(async move {
            if let Err(e) = self.inner_operate(req).await {
                println!("http数据请求时发生错误: {:?}", e);
            }
        });
        Ok((r, s))
    }

    pub async fn send_now(mut self, mut req: RecvRequest) -> ProtResult<RecvResponse> {
        self.rebuild_request(&mut req);
        let (mut r, s) = self.split()?;
        // let _ = self.operate(req).await;
        tokio::spawn(async move {
            let _sender = s;
            if let Err(e) = self.inner_operate(req).await {
                println!("http数据请求时发生错误: {:?}", e);
            }
        });
        if let Some(mut s) = r.recv().await {
            if let Ok(res) = &mut s {
                res.extensions_mut().insert(r);
            }
            return s;
        } else {
            return Err(ProtError::Extension("unknow response"));
        }
    }

    pub async fn recv(&mut self) -> ProtResult<RecvResponse> {
        if let Some(recv) = &mut self.receiver {
            if let Some(res) = recv.recv().await {
                res
            } else {
                Err(ProtError::Extension("recv close"))
            }
        } else {
            Err(ProtError::Extension("has not recv"))
        }
    }
}

// impl<T> Drop for Client<T>
// where
//     T: AsyncRead + AsyncWrite + Unpin + Send + 'static, {
//         fn drop(&mut self) {
//             println!("drop client!!!!!!!");
//             // drop(self.)
//         }
//     }
