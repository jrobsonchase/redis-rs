use std::collections::VecDeque;
use std::fmt::Arguments;
use std::io::{self, BufReader, Read, Write};
use std::mem;
use std::net::ToSocketAddrs;

#[cfg(unix)]
use tokio_uds::UnixStream;

use tokio_codec::{Decoder, Framed};
use tokio_executor;
use tokio_io::{self, AsyncWrite};
use tokio_tcp::TcpStream;

use futures3::channel::*;
use futures3::compat::*;
use futures3::future::BoxFuture;
use futures3::prelude::*;

use crate::cmd::cmd;
use crate::types::{ErrorKind, RedisError, RedisFuture, RedisResult, Value};

use crate::connection::{ConnectionAddr, ConnectionInfo};

use crate::parser::ValueCodec;

type RedisStream<S> = Compat01As03Sink<Framed<S, ValueCodec>, Vec<u8>>;

enum ActualConnection {
    Tcp(RedisStream<TcpStream>),
    #[cfg(unix)]
    Unix(RedisStream<UnixStream>),
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    db: i64,
}

macro_rules! with_connection {
    ($bind:ident: $con:expr => { $($f:expr)* }) => {
        match $con {
            ActualConnection::Tcp(ref mut $bind) => {$($f)*},
            #[cfg(unix)]
            ActualConnection::Unix(ref mut $bind) => {$($f)*},
        }
    };
}

impl Connection {
    pub async fn read_response(&mut self) -> Result<Value, RedisError> {
        let result: Result<Option<Value>, RedisError> =
            with_connection!(con: self.con => { TryStreamExt::try_next(con).await });

        if let Some(val) = result? {
            Ok(val)
        } else {
            fail!((ErrorKind::ResponseError, "no response"))
        }
    }
}

pub async fn connect(connection_info: ConnectionInfo) -> Result<Connection, RedisError> {
    let connection = match *connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = match (&host[..], port).to_socket_addrs()?.next() {
                Some(socket_addr) => socket_addr,
                None => {
                    fail!((ErrorKind::InvalidClientConfig, "No address found for host"));
                }
            };

            let stream = TcpStream::connect(&socket_addr).compat().await?;
            let framed = ValueCodec::default().framed(stream).sink_compat();
            ActualConnection::Tcp(framed)
        }
        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => {
            let stream = UnixStream::connect(path).compat().await?;
            let framed = ValueCodec::default().framed(stream).sink_compat();
            ActualConnection::Unix(framed)
        }
        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => fail!((
            ErrorKind::InvalidClientConfig,
            "Cannot connect to unix sockets on this platform",
        )),
    };

    let mut rv = Connection {
        con: connection,
        db: connection_info.db,
    };

    if let Some(ref passwd) = connection_info.passwd {
        let mut cmd = cmd("AUTH");
        cmd.arg(&**passwd);

        let val = cmd.query_async::<_, Value>(&mut rv).await?;
        if let Value::Okay = val {

        } else {
            fail!((
                ErrorKind::AuthenticationFailed,
                "Password authentication failed"
            ));
        }
    }

    if connection_info.db != 0 {
        let mut cmd = cmd("SELECT");
        cmd.arg(connection_info.db);
        let value = cmd.query_async::<_, Value>(&mut rv).await?;
        if let Value::Okay = value {
        } else {
            fail!((
                ErrorKind::ResponseError,
                "Redis server refused to switch database"
            ))
        }
    }

    Ok(rv)
}

pub trait ConnectionLike: Unpin + Sized {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(&mut self, cmd: Vec<u8>) -> BoxFuture<'_, Result<Value, RedisError>>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands(
        &mut self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> BoxFuture<'_, Result<Vec<Value>, RedisError>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}

impl ConnectionLike for Connection {
    fn req_packed_command(&mut self, cmd: Vec<u8>) -> BoxFuture<'_, RedisResult<Value>> {
        async move {
            with_connection!(con: self.con => {
                con.send(cmd).await?
            });
            self.read_response().await
        }
            .boxed()
    }

    fn req_packed_commands(
        &mut self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> BoxFuture<RedisResult<Vec<Value>>> {
        async move {
            with_connection!(con: self.con => {
                con.send(cmd).await?
            });
            let mut rv = vec![];
            for idx in 0..(count + offset) {
                let item = self.read_response().await?;

                if idx >= offset {
                    rv.push(item);
                }
            }
            Ok(rv)
        }
            .boxed()
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}
/*


// Senders which the result of a single request are sent through
type PipelineOutput<O, E> = oneshot::Sender<Result<Vec<O>, E>>;

struct InFlight<O, E> {
    output: PipelineOutput<O, E>,
    response_count: usize,
    buffer: Vec<O>,
}

// A single message sent through the pipeline
struct PipelineMessage<S, I, E> {
    input: S,
    output: PipelineOutput<I, E>,
    response_count: usize,
}

/// Wrapper around a `Stream + Sink` where each item sent through the `Sink` results in one or more
/// items being output by the `Stream` (the number is specified at time of sending). With the
/// interface provided by `Pipeline` an easy interface of request to response, hiding the `Stream`
/// and `Sink`.
struct Pipeline<T>(mpsc::Sender<PipelineMessage<T::SinkItem, T::Item, T::Error>>)
where
    T: Stream + Sink;

impl<T> Clone for Pipeline<T>
where
    T: Stream + Sink,
{
    fn clone(&self) -> Self {
        Pipeline(self.0.clone())
    }
}

struct PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    sink_stream: T,
    in_flight: VecDeque<InFlight<T::Item, T::Error>>,
}

impl<T> PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    // Read messages from the stream and send them back to the caller
    fn poll_read(&mut self) -> Poll<(), ()> {
        loop {
            let item = match self.sink_stream.poll() {
                Ok(Async::Ready(Some(item))) => Ok(item),
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                Ok(Async::Ready(None)) => return Err(()),
                Err(err) => Err(err),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
            };
            self.send_result(item);
        }
    }

    fn send_result(&mut self, result: Result<T::Item, T::Error>) {
        let response = {
            let entry = match self.in_flight.front_mut() {
                Some(entry) => entry,
                None => return,
            };
            match result {
                Ok(item) => {
                    entry.buffer.push(item);
                    if entry.response_count > entry.buffer.len() {
                        // Need to gather more response values
                        return;
                    }
                    Ok(mem::replace(&mut entry.buffer, Vec::new()))
                }
                // If we fail we must respond immediately
                Err(err) => Err(err),
            }
        };

        let entry = self.in_flight.pop_front().unwrap();
        match entry.output.send(response) {
            Ok(_) => (),
            Err(_) => {
                // `Err` means that the receiver was dropped in which case it does not
                // care about the output and we can continue by just dropping the value
                // and sender
            }
        }
    }
}

impl<T> Sink for PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    type SinkItem = PipelineMessage<T::SinkItem, T::Item, T::Error>;
    type SinkError = ();

    // Retrieve incoming messages and write them to the sink
    fn start_send(
        &mut self,
        PipelineMessage {
            input,
            output,
            response_count,
        }: Self::SinkItem,
    ) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.sink_stream.start_send(input) {
            Ok(AsyncSink::NotReady(input)) => Ok(AsyncSink::NotReady(PipelineMessage {
                input,
                output,
                response_count,
            })),
            Ok(AsyncSink::Ready) => {
                self.in_flight.push_back(InFlight {
                    output,
                    response_count,
                    buffer: Vec::new(),
                });
                Ok(AsyncSink::Ready)
            }
            Err(err) => {
                let _ = output.send(Err(err));
                Err(())
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        try_ready!(self.sink_stream.poll_complete().map_err(|err| {
            self.send_result(Err(err));
        }));
        self.poll_read()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        try_ready!(self.sink_stream.close().map_err(|err| {
            self.send_result(Err(err));
        }));
        self.poll_read()
    }
}

impl<T> Pipeline<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + Send + 'static,
    T::SinkItem: Send,
    T::Item: Send,
    T::Error: Send,
    T::Error: ::std::fmt::Debug,
{
    fn new(sink_stream: T) -> Self {
        const BUFFER_SIZE: usize = 50;
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        tokio_executor::spawn(
            receiver
                .map_err(|_| ())
                .forward(PipelineSink {
                    sink_stream,
                    in_flight: VecDeque::new(),
                })
                .map(|_| ()),
        );
        Pipeline(sender)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    fn send(
        &self,
        item: T::SinkItem,
    ) -> impl Future<Item = T::Item, Error = Option<T::Error>> + Send {
        self.send_recv_multiple(item, 1)
            .map(|mut item| item.pop().unwrap())
    }

    fn send_recv_multiple(
        &self,
        input: T::SinkItem,
        count: usize,
    ) -> impl Future<Item = Vec<T::Item>, Error = Option<T::Error>> + Send {
        let self_ = self.0.clone();

        let (sender, receiver) = oneshot::channel();

        self_
            .send(PipelineMessage {
                input,
                response_count: count,
                output: sender,
            })
            .map_err(|_| None)
            .and_then(|_| {
                receiver.then(|result| match result {
                    Ok(result) => result.map_err(Some),
                    Err(_) => {
                        // The `sender` was dropped which likely means that the stream part
                        // failed for one reason or another
                        Err(None)
                    }
                })
            })
    }
}

#[derive(Clone)]
enum ActualPipeline {
    Tcp(Pipeline<Framed<TcpStream, ValueCodec>>),
    #[cfg(unix)]
    Unix(Pipeline<Framed<UnixStream, ValueCodec>>),
}

#[derive(Clone)]
pub struct SharedConnection {
    pipeline: ActualPipeline,
    db: i64,
}

impl SharedConnection {
    pub fn new(con: Connection) -> impl Future<Item = Self, Error = RedisError> {
        future::lazy(|| {
            let pipeline = match con.con {
                ActualConnection::Tcp(tcp) => {
                    let codec = ValueCodec::default().framed(tcp.into_inner());
                    ActualPipeline::Tcp(Pipeline::new(codec))
                }
                #[cfg(unix)]
                ActualConnection::Unix(unix) => {
                    let codec = ValueCodec::default().framed(unix.into_inner());
                    ActualPipeline::Unix(Pipeline::new(codec))
                }
            };
            Ok(SharedConnection {
                pipeline,
                db: con.db,
            })
        })
    }
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        #[cfg(not(unix))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send(cmd),
        };

        #[cfg(unix)]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => Either::A(pipeline.send(cmd)),
            ActualPipeline::Unix(ref pipeline) => Either::B(pipeline.send(cmd)),
        };

        Box::new(future.map(|value| (self, value)).map_err(|err| {
            err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
        }))
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        #[cfg(not(unix))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send_recv_multiple(cmd, offset + count),
        };

        #[cfg(unix)]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => {
                Either::A(pipeline.send_recv_multiple(cmd, offset + count))
            }
            ActualPipeline::Unix(ref pipeline) => {
                Either::B(pipeline.send_recv_multiple(cmd, offset + count))
            }
        };

        Box::new(
            future
                .map(move |mut value| {
                    value.drain(..offset);
                    (self, value)
                })
                .map_err(|err| {
                    err.unwrap_or_else(|| {
                        RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
                    })
                }),
        )
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}

*/
