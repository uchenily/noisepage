#pragma once

#include <functional>
#include <memory>
#include <unordered_set>
#include <utility>
#include <variant> // NOLINT (Matt): lint thinks this C++17 header is a C header because it only knows C++11
#include <vector>

#include "common/managed_pointer.h"
#include "libcuckoo/cuckoohash_config.hh"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"

namespace noisepage::transaction {
class TransactionContext;
} // namespace noisepage::transaction

template <class Key, class T, class Hash, class KeyEqual, class Allocator, std::size_t SLOT_PER_BUCKET>
class cuckoohash_map;

namespace noisepage::storage::index {

template <uint16_t KeySize>
class HashKey;
template <uint16_t KeySize>
class GenericKey;

/**
 * Wrapper around libcuckoo's hash map. The MVCC is logic is similar to our reference index (BwTreeIndex). Much of the
 * logic here is related to the cuckoohash_map not being a multimap. We get around this by making the value type a
 * std::variant that can either be a TupleSlot if there's only a single value for a given key, or a std::unordered_set
 * of TupleSlots if a single key needs to map to multiple TupleSlots.
 * @tparam KeyType the type of keys stored in the map
 */
template <typename KeyType>
class HashIndex final : public Index {
    friend class IndexBuilder;

private:
    // TODO(Matt): unclear at the moment if we would want this to be tunable via the SettingsManager. Alternatively, it
    // might be something that is a per-index hint based on the table size (cardinality?), rather than a global setting
    static constexpr uint16_t INITIAL_CUCKOOHASH_MAP_SIZE = 256;
    struct TupleSlotHash;

    using ValueMap = std::unordered_set<TupleSlot, TupleSlotHash>;
    using ValueType = std::variant<TupleSlot, ValueMap>;

    explicit HashIndex(IndexMetadata metadata);

    const std::unique_ptr<cuckoohash_map<KeyType,
                                         ValueType,
                                         std::hash<KeyType>,
                                         std::equal_to<KeyType>, // NOLINT transparent functors can't figure out
                                                                 // template
                                         std::allocator<std::pair<const KeyType, ValueType>>,
                                         LIBCUCKOO_DEFAULT_SLOT_PER_BUCKET>>
                              hash_map_;
    mutable common::SpinLatch transaction_context_latch_; // latch used to protect transaction context

public:
    /**
     * @return type of the index. Note that this is the physical type, not extracted from the underlying schema or other
     * catalog metadata. This is mostly used for debugging purposes.
     */
    IndexType Type() const final {
        return IndexType::HASHMAP;
    }

    /**
     * @return approximate number of bytes allocated on the heap for this index data structure
     */
    size_t EstimateHeapUsage() const final;

    /**
     * Inserts a new key-value pair into the index, used for non-unique key indexes.
     * @param txn txn context for the calling txn, used to register abort actions
     * @param tuple key
     * @param location value
     * @return false if the value already exists, true otherwise
     */
    bool Insert(common::ManagedPointer<transaction::TransactionContext> txn,
                const ProjectedRow                                     &tuple,
                TupleSlot                                               location) final;

    /**
     * Inserts a key-value pair only if any matching keys have TupleSlots that don't conflict with the calling txn
     * @param txn txn context for the calling txn, used for visibility and write-write, and to register abort actions
     * @param tuple key
     * @param location value
     * @return true if the value was inserted, false otherwise
     *         (either because value exists, or predicate returns true for one of the existing values)
     */
    bool InsertUnique(common::ManagedPointer<transaction::TransactionContext> txn,
                      const ProjectedRow                                     &tuple,
                      TupleSlot                                               location) final;

    /**
     * Doesn't immediately call delete on the index. Registers a commit action in the txn that will eventually register
     * a deferred action for the GC to safely call delete on the index when no more transactions need to access the key.
     * @param txn txn context for the calling txn, used to register commit actions for deferred GC actions
     * @param tuple key
     * @param location value
     */
    void Delete(common::ManagedPointer<transaction::TransactionContext> txn,
                const ProjectedRow                                     &tuple,
                TupleSlot                                               location) final;

    /**
     * Finds all the values associated with the given key in our index.
     * @param txn txn context for the calling txn, used for visibility checks
     * @param key the key to look for
     * @param[out] value_list the values associated with the key
     */
    void ScanKey(const transaction::TransactionContext &txn,
                 const ProjectedRow                    &key,
                 std::vector<TupleSlot>                *value_list) final;

    /** @return The number of keys in the index. */
    uint64_t GetSize() const final;
};

extern template class HashIndex<HashKey<8>>;
extern template class HashIndex<HashKey<16>>;
extern template class HashIndex<HashKey<32>>;
extern template class HashIndex<HashKey<64>>;
extern template class HashIndex<HashKey<128>>;
extern template class HashIndex<HashKey<256>>;

extern template class HashIndex<GenericKey<64>>;
extern template class HashIndex<GenericKey<128>>;
extern template class HashIndex<GenericKey<256>>;

} // namespace noisepage::storage::index
