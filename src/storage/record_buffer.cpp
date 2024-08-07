#include "storage/record_buffer.h"

#include "storage/write_ahead_log/log_manager.h"

namespace noisepage::storage {
auto UndoBuffer::NewEntry(const uint32_t size) -> byte * {
    if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
        // we are out of space in the buffer. Get a new buffer segment.
        RecordBufferSegment *new_segment = buffer_pool_->Get();
        NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0,
                         "a delta entry should be aligned to 8 bytes");
        buffers_.push_back(new_segment);
    }
    last_record_ = buffers_.back()->Reserve(size);
    return last_record_;
}

auto RedoBuffer::NewEntry(const uint32_t size, const transaction::TransactionPolicy &policy) -> byte * {
    if (buffer_seg_ == nullptr) {
        // this is the first write
        buffer_seg_ = buffer_pool_->Get();
    } else if (!buffer_seg_->HasBytesLeft(size)) {
        // old log buffer is full
        if (log_manager_ != DISABLED && policy.durability_ != transaction::DurabilityPolicy::DISABLE) {
            log_manager_->AddBufferToFlushQueue(buffer_seg_, policy);
            has_flushed_ = true;
        } else {
            buffer_pool_->Release(buffer_seg_);
        }
        buffer_seg_ = buffer_pool_->Get();
    }
    NOISEPAGE_ASSERT(buffer_seg_->HasBytesLeft(size),
                     "Staged write does not fit into redo buffer (even after a fresh one is requested)");
    last_record_ = buffer_seg_->Reserve(size);
    return last_record_;
}

void RedoBuffer::Finalize(bool flush_buffer, const transaction::TransactionPolicy &policy) {
    if (buffer_seg_ == nullptr) {
        return; // If we never initialized a buffer (logging was disabled), we don't do anything
    }
    if (log_manager_ != DISABLED && flush_buffer && policy.durability_ != transaction::DurabilityPolicy::DISABLE) {
        log_manager_->AddBufferToFlushQueue(buffer_seg_, policy);
        has_flushed_ = true;
    } else {
        buffer_pool_->Release(buffer_seg_);
    }
}
} // namespace noisepage::storage
