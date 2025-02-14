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
// Created Date: 2023/08/31 10:06:46

mod server;
mod client;
pub mod http1;
pub mod http2;
mod error;
mod header_helper;
mod http_helper;
mod stream;
pub mod ws;

mod body;
mod send_stream;
mod consts;
mod layer;
mod middle;
mod proxy;
pub mod plugins;

use std::any::Any;

pub use self::body::Body;
pub use self::send_stream::SendStream;
pub use self::stream::MaybeHttpsStream;

pub use self::client::{Client, ClientOption};
pub use self::server::Server;
pub use self::error::{ProtResult, ProtError, Initiator};
pub use self::http2::{Builder, ServerH2Connection, StateHandshake, SendControl};
pub use self::header_helper::HeaderHelper;
pub use self::consts::Consts;
pub use self::http_helper::HttpHelper;
pub use self::layer::{RateLimitLayer, TimeoutLayer, Rate};
pub use self::middle::Middleware;
pub use self::proxy::ProxyScheme;


use webparse::{Request, Response};

pub type RecvRequest = Request<Body>;
pub type RecvResponse = Response<Body>;

use async_trait::async_trait;

#[async_trait]
pub trait HttpTrait: Send + Sync + Any {
    /// 处理请求并返回正确的数据
    async fn operate(&mut self, mut req: RecvRequest) -> ProtResult<RecvResponse>;
    
    /// 处理中间件的请求，跟中间件相关的处理
    async fn middle_operate(&mut self, req: &mut RecvRequest, middles: &mut Vec<Box<dyn Middleware>>) -> ProtResult<()> {
        let _req = req;
        let _middle = middles;
        Ok(())
    }

    async fn close_connect(&mut self) {
        
    }

    /// 是否主动结束服务，返回false则表示服务暂停
    fn is_continue_next(&self) -> bool {
        true
    }

    fn as_any(&self) -> Option<&dyn Any> {
        None
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        None
    }
}