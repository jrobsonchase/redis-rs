use std::io::{self, BufRead};
use std::pin::Pin;
use std::str;

use crate::types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use futures3::compat::*;
use futures3::future::poll_fn;
use futures3::prelude::*;
use futures3::task::Poll;

use bytes::BytesMut;
use futures::{Async, Future};
use tokio_io::codec::{Decoder, Encoder};

use combine;
use combine::byte::{byte, crlf, take_until_bytes};
use combine::combinator::{any_send_partial_state, AnySendPartialState};
#[allow(unused_imports)] // See https://github.com/rust-lang/rust/issues/43970
use combine::error::StreamError;
use combine::parser::choice::choice;
use combine::range::{recognize, take};
use combine::stream::{FullRangeStream, StreamErrorFor};

struct ResultExtend<T, E>(Result<T, E>);

impl<T, E> Default for ResultExtend<T, E>
where
    T: Default,
{
    fn default() -> Self {
        ResultExtend(Ok(T::default()))
    }
}

impl<T, U, E> Extend<Result<U, E>> for ResultExtend<T, E>
where
    T: Extend<U>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<U, E>>,
    {
        let mut returned_err = None;
        match self.0 {
            Ok(ref mut elems) => elems.extend(iter.into_iter().scan((), |_, item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    returned_err = Some(err);
                    None
                }
            })),
            Err(_) => (),
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

parser! {
    type PartialState = AnySendPartialState;
    fn value['a, I]()(I) -> RedisResult<Value>
        where [I: FullRangeStream<Item = u8, Range = &'a [u8]> ]
    {
        let line = || recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ())))
            .and_then(|line: &[u8]| {
                str::from_utf8(&line[..line.len() - 2])
                    .map_err(StreamErrorFor::<I>::other)
            });

        let status = || line().map(|line| {
            if line == "OK" {
                Value::Okay
            } else {
                Value::Status(line.into())
            }
        });

        let int = || line().and_then(|line| {
            match line.trim().parse::<i64>() {
                Err(_) => Err(StreamErrorFor::<I>::message_static_message("Expected integer, got garbage")),
                Ok(value) => Ok(value),
            }
        });

        let data = || int().then_partial(move |size| {
            if *size < 0 {
                combine::value(Value::Nil).left()
            } else {
                take(*size as usize)
                    .map(|bs: &[u8]| Value::Data(bs.to_vec()))
                    .skip(crlf())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(|&mut length| {
                if length < 0 {
                    combine::value(Value::Nil).map(Ok).left()
                } else {
                    let length = length as usize;
                    combine::count_min_max(length, length, value())
                        .map(|result: ResultExtend<_, _>| {
                            result.0.map(Value::Bulk)
                        })
                        .right()
                }
            })
        };

        let error = || {
            line()
                .map(|line: &str| {
                    let desc = "An error was signalled by the server";
                    let mut pieces = line.splitn(2, ' ');
                    let kind = match pieces.next().unwrap() {
                        "ERR" => ErrorKind::ResponseError,
                        "EXECABORT" => ErrorKind::ExecAbortError,
                        "LOADING" => ErrorKind::BusyLoadingError,
                        "NOSCRIPT" => ErrorKind::NoScriptError,
                        code => {
                            return make_extension_error(code, pieces.next())
                        }
                    };
                    match pieces.next() {
                        Some(detail) => RedisError::from((kind, desc, detail.to_string())),
                        None => RedisError::from((kind, desc)),
                    }
                })
        };

        any_send_partial_state(choice((
           byte(b'+').with(status().map(Ok)),
           byte(b':').with(int().map(Value::Int).map(Ok)),
           byte(b'$').with(data().map(Ok)),
           byte(b'*').with(bulk()),
           byte(b'-').with(error().map(Err))
        )))
    }
}

#[derive(Default)]
pub struct ValueCodec {
    state: AnySendPartialState,
}

impl Encoder for ValueCodec {
    type Item = Vec<u8>;
    type Error = RedisError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(item);
        Ok(())
    }
}

impl Decoder for ValueCodec {
    type Item = Value;
    type Error = RedisError;
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (opt, removed_len) = {
            let buffer = &bytes[..];
            let stream = combine::easy::Stream(combine::stream::PartialStream(buffer));
            match combine::stream::decode(value(), stream, &mut self.state) {
                Ok(x) => x,
                Err(err) => {
                    let err = err
                        .map_position(|pos| pos.translate_position(buffer))
                        .map_range(|range| format!("{:?}", range))
                        .to_string();
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "parse error",
                        err,
                    )));
                }
            }
        };

        bytes.split_to(removed_len);
        match opt {
            Some(result) => Ok(Some(result?)),
            None => Ok(None),
        }
    }
}

/// The internal redis response parser.
pub struct Parser<T> {
    reader: T,
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl<'a, T: BufRead> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    // public api

    pub fn parse_value(&mut self) -> RedisResult<Value> {
        unimplemented!()
        // futures3::executor::block_on(parse_async(&mut self.reader))
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new(bytes);
    parser.parse_value()
}
