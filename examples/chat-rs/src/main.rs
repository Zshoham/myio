//! chat-rs - the persistent peer-to-peer chat from chat.c written with
//! tokio's async/await, for comparison with the myio version.
//!
//! usage: chat-rs <port> [host [peer-port]]
//!
//! Same behavior as examples/chat.c: always listens on <port>, dials
//! host:peer-port when a host is given (retrying every 10 seconds, override
//! with CHAT_RETRY_MS), returns to waiting when the peer leaves, ctrl-d
//! quits.
//!
//! Structurally this is the closest of the comparisons to the myio version:
//! `tokio::select!` over the pending operations of the current state is the
//! same shape as myio_select() over task slots. The notable difference is
//! cancellation: dropping a future (the select loser, or the in-flight dial
//! when a connection is accepted) cancels and cleans it up implicitly,
//! where the C versions must cancel/close explicitly.

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

fn retry_interval() -> Duration {
    let ms = std::env::var("CHAT_RETRY_MS") // test hook
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    Duration::from_millis(ms)
}

/// Dial until a connection is established; pends forever without a host.
async fn dial_forever(host: Option<&str>, port: u16, retry: Duration) -> TcpStream {
    let Some(host) = host else {
        return std::future::pending().await;
    };
    loop {
        match TcpStream::connect((host, port)).await {
            Ok(s) => return s,
            Err(e) => {
                eprintln!(
                    "* connect to {host}:{port} failed ({e}), retrying in {} ms",
                    retry.as_millis()
                );
                sleep(retry).await;
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args.len() > 4 {
        eprintln!("usage: {} <port> [host [peer-port]]", args[0]);
        std::process::exit(2);
    }
    let port: u16 = args[1].parse().expect("bad port");
    let host = args.get(2).map(|s| s.as_str());
    let peer_port: u16 = args.get(3).map_or(port, |s| s.parse().expect("bad peer port"));
    let retry = retry_interval();

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap_or_else(|e| {
        eprintln!("* cannot listen on port {port}: {e}");
        std::process::exit(1);
    });
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let mut buf = [0u8; 512];

    eprintln!("* type messages; ctrl-d quits");
    loop {
        eprintln!(
            "* waiting for a peer on port {port}{}",
            if host.is_some() { " (and dialing out)" } else { "" }
        );

        // Disconnected: race the listener, the dialer and stdin.
        let stream = {
            let dial = dial_forever(host, peer_port, retry);
            tokio::pin!(dial);
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((s, _)) => break s,
                        Err(e) => eprintln!("* accept failed: {e}"),
                    },
                    dialed = &mut dial => break dialed,
                    line = stdin.next_line() => match line {
                        Ok(Some(_)) => eprintln!("* not connected, message dropped"),
                        _ => {
                            eprintln!("* stdin closed, bye");
                            return;
                        }
                    },
                }
            }
        };
        eprintln!("* peer connected");
        let (mut rd, mut wr) = stream.into_split();

        // Connected: race the peer and stdin.
        loop {
            tokio::select! {
                n = rd.read(&mut buf) => match n {
                    Ok(n) if n > 0 => {
                        stdout.write_all(b"peer> ").await.ok();
                        stdout.write_all(&buf[..n]).await.ok();
                        stdout.flush().await.ok();
                    }
                    _ => {
                        eprintln!("* peer disconnected");
                        break;
                    }
                },
                line = stdin.next_line() => match line {
                    Ok(Some(line)) => {
                        let msg = format!("{line}\n");
                        if let Err(e) = wr.write_all(msg.as_bytes()).await {
                            eprintln!("* send failed: {e}");
                            break;
                        }
                    }
                    _ => {
                        eprintln!("* stdin closed, bye");
                        return;
                    }
                },
            }
        }
    }
}
