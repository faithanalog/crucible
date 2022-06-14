use std::convert::TryInto;
// Copyright 2021 Oxide Computer Company
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Result};
use structopt::StructOpt;
use tokio::runtime::Builder;
use tokio::task;

use crucible::*;

use std::net::{TcpListener, TcpStream as NetTcpStream};

use async_trait::async_trait;
use std::io::Result as IOResult;
use lazy_static::lazy_static;
use nbdkit::{CacheFlags, ExtentHandle, Flags, FuaFlags, plugin, Server, ThreadModel};
use uuid::Uuid;
use std::sync::Mutex;




/*
 * Wrap a Crucible volume and implement Read + Write + Seek traits.
 */
pub struct CrucibleDev<T> {
    active: bool,
    block_io: Arc<T>,
    sz: u64,
    block_size: u64,
    uuid: Uuid,
}

// impl<T> Copy for CrucibleDev<T> {}

impl<T> Clone for CrucibleDev<T> {
    fn clone(&self) -> Self {
       CrucibleDev {
           active: self.active,
           block_io: self.block_io.clone(),
           sz: self.sz,
           block_size: self.block_size,
           uuid: self.uuid.clone()
       }
    }
}

impl<T: BlockIO> CrucibleDev<T> {
    pub fn from(block_io: Arc<T>) -> Result<Self, CrucibleError> {
        Ok(CrucibleDev {
            active: false,
            block_io,
            sz: 0,
            block_size: 0,
            uuid: Uuid::default(),
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn sz(&self) -> u64 {
        self.sz
    }

    pub fn activate(&mut self, gen: u64) -> Result<(), CrucibleError> {
        if let Err(e) = self.block_io.activate(gen) {
            match e {
                CrucibleError::UpstairsAlreadyActive => {
                    // underlying block io is already active, but pseudo file
                    // needs fields below populated
                }
                _ => {
                    return Err(e);
                }
            }
        }

        self.sz = self.block_io.total_size()?;
        self.block_size = self.block_io.get_block_size()?;
        self.uuid = self.block_io.get_uuid()?;

        self.active = true;

        Ok(())
    }

    pub fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        self.block_io.show_work()
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl<T: BlockIO> CrucibleDev<T> {
    fn _read(&self, offset: u64, buf: &mut [u8]) -> Result<(), CrucibleError> {
        let mut span =
            IOSpan::new(offset, buf.len() as u64, self.block_size);

        let mut waiter =
            span.read_affected_blocks_from_volume(&self.block_io)?;

        waiter.block_wait()?;

        span.read_from_blocks_into_buffer(buf);

        Ok(())
    }

    fn _write(&self, offset: u64, buf: &[u8]) -> Result<(), CrucibleError> {
        let mut span =
            IOSpan::new(offset, buf.len() as u64, self.block_size);

        /*
         * Crucible's dependency system will properly resolve requests in
         * the order they are received but if the request is not block
         * aligned and block sized we need to do read-modify-write (RMW)]
         * here. Use a reader-writer lock, and grab the write portion of
         * the lock when doing RMW to cause all other operations (which
         * only grab the read portion
         * of the lock) to pause. Otherwise all operations can use the
         * read portion of this lock and Crucible will sort it out.
         */
        if !span.is_block_regular() {

            let mut waiter =
                span.read_affected_blocks_from_volume(&self.block_io)?;
            waiter.block_wait()?;

            span.write_from_buffer_into_blocks(buf);

            let mut waiter =
                span.write_affected_blocks_to_volume(&self.block_io)?;
            waiter.block_wait()?;
        } else {
            let offset = Block::new(
                offset / self.block_size,
                self.block_size.trailing_zeros(),
            );
            let bytes = BytesMut::from(buf);
            let mut waiter = self.block_io.write(offset, bytes.freeze())?;
            waiter.block_wait()?;
        }

        Ok(())
    }

    fn _flush(&self) -> Result<(), CrucibleError> {
        let mut waiter = self.block_io.flush(None)?;
        waiter.block_wait()?;

        Ok(())
    }
}


#[derive(Debug, StructOpt)]
#[structopt(about = "volume-side storage component")]
pub struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:9000")]
    target: Vec<SocketAddr>,

    #[structopt(short, long)]
    key: Option<String>,

    #[structopt(short, long, default_value = "0")]
    gen: u64,

    // TLS options
    #[structopt(long)]
    cert_pem: Option<String>,
    #[structopt(long)]
    key_pem: Option<String>,
    #[structopt(long)]
    root_cert_pem: Option<String>,

    // Start upstairs control http server
    #[structopt(long)]
    control: Option<SocketAddr>,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::from_args();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

// lazy_static! {
//     static ref DISK: Mutex<Vec<u8>> = Mutex::new (vec![0; 100 * 1024 * 1024]);
// }
lazy_static! {
    static ref DEVICE: Mutex<Option<CrucibleDev<Guest>>> = Mutex::new(Option::None);
    static ref UPSTAIRS: Mutex<tokio::runtime::Runtime> = Mutex::new(
        Builder::new_multi_thread()
            .worker_threads(10)
            .thread_name("crucible-tokio")
            .enable_all()
            .build()
            .unwrap()
    );
}

struct CrucibleServer {
    server_dev: CrucibleDev<Guest>
}

impl Server for CrucibleServer {
    fn get_size(&self) -> nbdkit::Result<i64> {
        let sz = self.server_dev.sz();
        let szi = sz.try_into().unwrap();
        Ok(szi)
    }

    fn name() -> &'static str {
        "crucible"
    }

    fn open(_readonly: bool) -> Box<dyn Server> {
        let dev = DEVICE.lock().unwrap();
        let dev = dev.as_ref();
        let dev = dev.unwrap().clone();
        Box::new(CrucibleServer{
            server_dev: dev
        })
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> nbdkit::Result<()> {
        let res = self.server_dev._read(offset, buf).map_err(|e| Into::<std::io::Error>::into(e).into());
        res
    }

    fn thread_model() -> nbdkit::Result<ThreadModel> where Self: Sized {
        Ok(ThreadModel::Parallel)
    }

    fn write_at(&self, buf: &[u8], offset: u64, _flags: Flags) -> nbdkit::Result<()> {
        let res = self.server_dev._write(offset, buf).map_err(|e| Into::<std::io::Error>::into(e).into());
        res
    }

    fn flush(&self) -> nbdkit::Result<()> {
        let res = self.server_dev._flush().map_err(|e| Into::<std::io::Error>::into(e).into());
        res
    }

    fn can_multi_conn(&self) -> nbdkit::Result<bool> {
        Ok(true)
    }

    // Runs when the plugin is first loaded.
    fn load() where Self: Sized {
        let crucible_opts = CrucibleOpts {
            target: vec!["172.16.254.177:3810".parse().unwrap(), "172.16.254.177:3820".parse().unwrap(), "172.16.254.177:3830".parse().unwrap()],
            lossy: false,
            key: None,
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
        };
        let runtime = UPSTAIRS.lock().unwrap();
        /*
         * The structure we use to send work from outside crucible into the
         * Upstairs main task.
         * We create this here instead of inside up_main() so we can use
         * the methods provided by guest to interact with Crucible.
         */
        let guest = Arc::new(Guest::new());

        runtime.spawn(up_main(crucible_opts, guest.clone()));

        println!("Crucible runtime is spawned");

        guest.activate(0).unwrap();
        let mut cpf = CrucibleDev::from(guest).unwrap();

        // sent to NBD client during handshake through Export struct
        cpf.activate(0).unwrap();

        let mut dev = DEVICE.lock().unwrap();
        *dev = Some(cpf);
    }
}

plugin!(CrucibleServer {thread_model, write_at, flush, load, can_multi_conn});
