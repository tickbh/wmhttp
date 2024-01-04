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
// Created Date: 2023/09/14 09:42:25

use std::io;
use std::pin::Pin;
use std::task::{ready, Poll};

use bytes::{BufMut, BytesMut, Buf};
use tokio::io::AsyncRead;
use tokio_stream::Stream;
use tokio_util::codec::FramedRead as InnerFramedRead;
use tokio_util::codec::LengthDelimitedCodec;

use webparse::http::http2::frame::{Frame, Kind};
use webparse::http::http2::{frame, Decoder};
use webparse::http2::DEFAULT_SETTINGS_HEADER_TABLE_SIZE;
use webparse::{Binary, BinaryMut, Buf, DataFrame, OwnedMessage, BinaryRef};

use crate::ProtResult;

#[derive(Debug)]
struct MyCodec;

impl tokio_util::codec::Decoder for MyCodec {
    // ...
    type Item = DataFrame;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        
        let frame = DataFrame::read_dataframe_with_limit(&mut BinaryRef::from(src.chunk()), false, 100000);
        // ...
        // Reserve enough to complete decoding of the current frame.
        let current_frame_len: usize = 1000; // Example.
                                             // And to start decoding the next frame.
        let next_frame_header_len: usize = 10; // Example.
        src.reserve(current_frame_len + next_frame_header_len);
        return Ok(None);
    }
}

#[derive(Debug)]
pub struct FramedRead<T> {
    inner: InnerFramedRead<T, MyCodec>,
    caches: Vec<DataFrame>,

}

impl<T> FramedRead<T> {
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }
}

impl<T> FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(io: T) -> FramedRead<T> {
        FramedRead {
            inner: InnerFramedRead::new(io, MyCodec),
            caches: vec![],
        }
    }

    pub fn into_io(self) -> T {
        self.inner.into_inner()
    }

    pub fn set_cache_buf(&mut self, read_buf: BinaryMut) {
        self.inner.read_buffer_mut().put_slice(read_buf.chunk());
    }
}

impl<T> AsyncRead for FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        use bytes::Buf;
        if self.inner.read_buffer_mut().remaining() > 0 {
            let read = std::cmp::min(buf.remaining(), self.inner.read_buffer_mut().remaining());
            buf.put_slice(&self.inner.read_buffer_mut().chunk()[..read]);
            self.inner.read_buffer_mut().advance(read);
            return Poll::Ready(Ok(()));
        }
        Pin::new(self.get_mut().get_mut()).poll_read(cx, buf)
    }
}

impl<T> Stream for FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    type Item = ProtResult<OwnedMessage>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let bytes = match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                None => return Poll::Ready(None),
            };

            let is_finish = bytes.finished;
            self.caches.push(bytes);
            if is_finish {
                let msg = OwnedMessage::from_dataframes(self.caches.drain(..).collect())?;
                return Poll::Ready(Some(Ok(msg)));
            } else {
                return Poll::Pending;
            }
        }
    }
}
