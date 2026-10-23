//! SCM_RIGHTS framing on a negotiated registered connection. No ordinary read may consume its bytes.
//! Linux unix(7): rights accompany stream bytes, not application frames; one byte carries each set.
use serde::{Serialize, de::DeserializeOwned};
use std::{
    io,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
};
use tokio::{io::Interest, net::UnixStream};

const MAX_RIGHTS: usize = 8;

pub async fn send<T: Serialize>(
    stream: &UnixStream,
    message: &T,
    files: &[BorrowedFd<'_>],
) -> io::Result<()> {
    if files.len() > MAX_RIGHTS {
        return Err(io::Error::other("too many attachment descriptors"));
    }
    let payload = bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(io::Error::other)?;
    if payload.len() > crate::MAX_FRAME {
        return Err(io::Error::other("oversized attachment frame"));
    }
    let mut bytes = Vec::with_capacity(10 + payload.len());
    bytes.extend(crate::FRAME_MAGIC);
    bytes.extend(crate::FRAME_VERSION.to_be_bytes());
    bytes.extend((payload.len() as u32).to_be_bytes());
    bytes.extend(payload);
    // A one-byte sendmsg either transfers its rights once or makes no progress (EAGAIN/EINTR).
    loop {
        stream.writable().await?;
        match stream.try_io(Interest::WRITABLE, || send_once(stream, &bytes[..1], files)) {
            Ok(1) => break,
            Ok(_) => return Err(io::Error::other("attachment first byte not sent")),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err(error),
        }
    }
    let mut offset = 1;
    while offset < bytes.len() {
        stream.writable().await?;
        match stream.try_io(Interest::WRITABLE, || {
            send_once(stream, &bytes[offset..], &[])
        }) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => offset += count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub async fn receive<T: DeserializeOwned>(stream: &UnixStream) -> io::Result<(T, Vec<OwnedFd>)> {
    receive_frame(stream, true).await
}
/// Replies carry no descriptors. Darwin inspects ancillary metadata without
/// installing FDs before consuming bytes, so client hosts have no inheritance window.
pub async fn receive_without_rights<T: DeserializeOwned>(
    stream: &UnixStream,
) -> io::Result<(T, Vec<OwnedFd>)> {
    receive_frame(stream, false).await
}
async fn receive_frame<T: DeserializeOwned>(
    stream: &UnixStream,
    allow_rights: bool,
) -> io::Result<(T, Vec<OwnedFd>)> {
    let mut header = [0u8; 10];
    let descriptors = read_part(stream, &mut header[..1], allow_rights).await?;
    read_part(stream, &mut header[1..], false).await?;
    if header[..4] != crate::FRAME_MAGIC
        || u16::from_be_bytes([header[4], header[5]]) != crate::FRAME_VERSION
    {
        return Err(io::Error::other("attachment frame version/magic mismatch"));
    }
    let size = u32::from_be_bytes(header[6..10].try_into().unwrap()) as _;
    if size > crate::MAX_FRAME {
        return Err(io::Error::other("oversized attachment frame"));
    }
    let mut payload = vec![0; size];
    read_part(stream, &mut payload, false).await?;
    let (message, used) = bincode::serde::decode_from_slice(&payload, bincode::config::standard())
        .map_err(io::Error::other)?;
    if used != size {
        return Err(io::Error::other("attachment frame trailing bytes"));
    }
    Ok((message, descriptors))
}
async fn read_part(
    stream: &UnixStream,
    mut buffer: &mut [u8],
    rights_allowed: bool,
) -> io::Result<Vec<OwnedFd>> {
    let mut descriptors = Vec::new();
    while !buffer.is_empty() {
        stream.readable().await?;
        match stream.try_io(Interest::READABLE, || {
            receive_once_with_rights(stream, buffer, rights_allowed)
        }) {
            Ok((0, _)) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok((count, received)) => {
                if !rights_allowed && !received.is_empty() {
                    return Err(io::Error::other("rights outside first frame byte"));
                }
                descriptors.extend(received);
                buffer = &mut buffer[count..];
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(descriptors)
}
fn send_once(stream: &UnixStream, bytes: &[u8], files: &[BorrowedFd<'_>]) -> io::Result<usize> {
    let mut vector = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    // usize-aligned storage satisfies cmsghdr alignment (cmsg(3)); only initialized lengths are exposed.
    let mut control = [0usize; 16];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    if !files.is_empty() {
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen =
            unsafe { libc::CMSG_SPACE((files.len() * std::mem::size_of::<libc::c_int>()) as u32) }
                as _;
        // BorrowedFd has native fd size, but marshal explicit integers, never its Rust representation.
        let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        unsafe {
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len =
                libc::CMSG_LEN((files.len() * std::mem::size_of::<libc::c_int>()) as u32) as _;
            let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
            for (index, file) in files.iter().enumerate() {
                data.add(index).write(file.as_raw_fd());
            }
        }
    }
    let count = unsafe { libc::sendmsg(stream.as_raw_fd(), &message, libc::MSG_NOSIGNAL) };
    if count < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(count as usize)
    }
}
fn receive_once_with_rights(
    stream: &UnixStream,
    bytes: &mut [u8],
    allow_rights: bool,
) -> io::Result<(usize, Vec<OwnedFd>)> {
    #[cfg(target_os = "macos")]
    let _inheritance = inheritance_guard()?;
    let mut vector = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    let mut control = [0usize; 16];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen =
        unsafe { libc::CMSG_SPACE((MAX_RIGHTS * std::mem::size_of::<libc::c_int>()) as u32) } as _;
    #[cfg(target_os = "macos")]
    {
        // XNU soreceive_ctl/sopeek_scm_rights returns ancillary metadata with
        // zeroed payload on MSG_PEEK, without externalizing any descriptors.
        // Never interpret those zeros as FDs. With one reader, consume no more
        // than the peeked bytes: newly appended rights cannot cross this boundary.
        let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut message, libc::MSG_PEEK) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        if message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
            || (!allow_rights && message.msg_controllen != 0)
        {
            // Closing the read side releases queued rights without installing
            // them. A null control buffer would silently discard their framing
            // (and some Darwin versions externalize even with no control buffer).
            if unsafe { libc::shutdown(stream.as_raw_fd(), libc::SHUT_RD) } != 0 {
                return Err(io::Error::last_os_error());
            }
            return Err(io::Error::other(
                "truncated or forbidden ancillary descriptors",
            ));
        }
        vector.iov_len = count as usize;
        message.msg_iov = &mut vector;
        message.msg_controllen =
            unsafe { libc::CMSG_SPACE((MAX_RIGHTS * std::mem::size_of::<libc::c_int>()) as u32) }
                as _;
        message.msg_flags = 0;
    }
    let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut message, receive_flags()) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut files = Vec::new();
    let mut valid = true;
    let mut header = unsafe { libc::CMSG_FIRSTHDR(&message) };
    while !header.is_null() {
        unsafe {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let size = (*header).cmsg_len.saturating_sub(libc::CMSG_LEN(0) as _);
                valid &= (size as usize).is_multiple_of(std::mem::size_of::<libc::c_int>());
                let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
                for index in 0..size as usize / std::mem::size_of::<libc::c_int>() {
                    files.push(OwnedFd::from_raw_fd(data.add(index).read()));
                }
            } else {
                valid = false;
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    // Received descriptors are RAII-owned before every rejection, including MSG_CTRUNC.
    if !valid
        || message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
        || files.len() > MAX_RIGHTS
        || (!allow_rights && !files.is_empty())
    {
        return Err(io::Error::other(
            "truncated or invalid ancillary descriptors",
        ));
    }
    #[cfg(target_os = "macos")]
    for file in &files {
        // Child spawn boundaries independently exclude non-allowlisted descriptors;
        // this flag alone cannot close Darwin's recvmsg/fcntl inheritance window.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        if flags < 0
            || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((count as usize, files))
}

#[cfg(test)]
mod tests;

#[cfg(target_os = "linux")]
fn receive_flags() -> i32 {
    libc::MSG_CMSG_CLOEXEC
}
#[cfg(target_os = "macos")]
fn receive_flags() -> i32 {
    0
}

/// Serialize Darwin recvmsg + CLOEXEC with every legacy supervisor fork. Native
/// posix_spawn additionally uses CLOEXEC_DEFAULT. Never hold across an await.
#[cfg(target_os = "macos")]
static INHERITANCE: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[cfg(target_os = "macos")]
pub fn inheritance_guard() -> io::Result<std::sync::MutexGuard<'static, ()>> {
    INHERITANCE
        .lock()
        .map_err(|_| io::Error::other("descriptor inheritance lock poisoned"))
}
