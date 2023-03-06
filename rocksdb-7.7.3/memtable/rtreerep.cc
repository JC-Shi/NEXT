// rtree_rep.cc is used to link the RTree_mem.h to the memtablerep.h
// (i.e., to facilitate rocksdb memtable using the rtree template)

#include "db/memtable.h"
#include "memtable/RTree_mem.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"
#include "util/rtree.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class RtreeRep : public MemTableRep {
    typedef int ValueType;
    typedef RTree<ValueType, int, 2, float> MyTree;
    MyTree rtree_;

 public:
    RtreeRep(Allocator* allocator);

    // Insert key into the rtree.
    // The parameter to insert is a single buffer contains key and value.
    // The insert parameter here follow other data format.
    void Insert(KeyHandle handle) override {
        auto* key_ = static_cast<char*>(handle);
        Slice key = ExtractUserKey(key_);
        Mbr mbr = ReadKeyMbr(key);

        Rect rect(mbr.first.min, mbr.first.max, mbr.second.min, mbr.second.max);
        rtree_.Insert(rect.min, rect.max, mbr.iid.min);
    }

    bool Contains(const char* key) const override;

    size_t ApproximateMemoryUsage() override {return 0;}

    void Get(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) override;
    
    ~RtreeRep() override {}

    //TODO: class Iterator

    // Returns an iterator over the keys
    MemTableRep::Iterator* GetIterator(IteratorContext* iterator_context, Arena* arena) override;


};

// void RTreeRep::Insert(KeyHandle handle) {
//     auto* key_ = static_cast<char*>(handle);
//     Slice internal_key = GetLengthPrefixedSlice(key_);
//     Slice key = ExtractUserKey(internal_key);
//     Mbr mbr = ReadKeyMbr(key);

//     Rect rect(mbr.first.min, mbr.first.max, mbr.second.min, mbr.second.max);
//     rtree_.Insert(rect.min, rect.max, mbr.iid.min);
// }

RtreeRep::RtreeRep (Allocator* allocator) 
    : MemTableRep (allocator), 
      rtree_() {}

bool MySearchCallback(int id)
{
    (void) id;
    return true; // keep going
}

bool RtreeRep::Contains(const char* key) const {
    Slice user_key = ExtractUserKey(key);
    Mbr key_mbr = ReadKeyMbr(user_key);
    Rect key_rect(key_mbr.first.min, key_mbr.first.max, key_mbr.second.min, key_mbr.second.max);

    int nhits;
    nhits = rtree_.Search(key_rect.min, key_rect.max, MySearchCallback);
    
    if (nhits > 0) {
        return true;
    } else {
        return false;
    }

}


void RtreeRep::Get(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) {
        (void) k;
        (void) callback_args;
        (void) callback_func;
    }

MemTableRep::Iterator* RtreeRep::GetIterator(IteratorContext* iterator_context, Arena* arena) {
    (void) iterator_context;
    (void) arena;
    return 0;
}

}

MemTableRep* RTreeFactory::CreateMemTableRep(
        const MemTableRep::KeyComparator&, Allocator* allocator,
        const SliceTransform*, Logger* /*logger*/) {
    return new RtreeRep(allocator);
}



} //namespace ROCKSDB_NAMESPACE
