use crate::resp::Frame;

pub const MAX_BUFFERED_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_RESPONSE_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_BULK_LEN: usize = MAX_BUFFERED_FRAME_BYTES;

pub fn resp_bulk_len(len: usize) -> Option<usize> {
    1usize
        .checked_add(decimal_len_usize(len))?
        .checked_add(2)?
        .checked_add(len)?
        .checked_add(2)
}

pub fn resp_array_header_len(items: usize) -> Option<usize> {
    1usize.checked_add(decimal_len_usize(items))?.checked_add(2)
}

pub fn checked_add_response_len(total: &mut usize, add: usize) -> bool {
    match total.checked_add(add) {
        Some(new_total) if new_total <= MAX_RESPONSE_FRAME_BYTES => {
            *total = new_total;
            true
        }
        _ => false,
    }
}

pub fn response_too_large() -> Frame {
    Frame::Error(format!(
        "ERR response exceeds output limit of {MAX_RESPONSE_FRAME_BYTES} bytes"
    ))
}

pub fn decimal_len_usize(n: usize) -> usize {
    n.to_string().len()
}

pub fn decimal_len_i64(n: i64) -> usize {
    n.to_string().len()
}
