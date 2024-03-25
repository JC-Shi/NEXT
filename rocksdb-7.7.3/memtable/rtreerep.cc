// rtree_rep.cc is used to link the RTree_mem.h to the memtablerep.h
// (i.e., to facilitate rocksdb memtable using the rtree template)

#include <iostream>

#include "db/memtable.h"
#include "util/RTree_mem.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"
#include "util/rtree.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class RtreeRep : public MemTableRep {
    typedef char* ValueType;
    typedef RTree<ValueType, double, 2, double> MyTree;
    MyTree rtree_;

 public:
    RtreeRep(Allocator* allocator);

    // Insert key into the rtree.
    // The parameter to insert is a single buffer contains key and value.
    // The insert parameter here follow other data format.
    void Insert(KeyHandle handle) override {
        auto* key_ = static_cast<char*>(handle);
        Slice internal_key = GetLengthPrefixedSlice(key_);
        Slice key = ExtractUserKey(internal_key);
        Mbr mbr = ReadKeyMbr(key);

        Rect rect(mbr.first.min, mbr.second.min, mbr.first.max, mbr.second.max);
        rtree_.Insert(rect.min, rect.max, key_);
    }

    bool Contains(const char* key) const override;

    size_t ApproximateMemoryUsage() override {return 0;}

    void Get(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) override;

    void SpatialRange(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) override;    
    
    ~RtreeRep() override {}

    class Iterator : public MemTableRep::Iterator {
        // MyTree::Iterator iter_;
     public:
        
        explicit Iterator( MyTree* rtree,
        IteratorContext* iterator_context ) {
            rtree_i = rtree;
            if (iterator_context != nullptr) {
                RtreeIteratorContext* context = 
                reinterpret_cast<RtreeIteratorContext*>(iterator_context);
                Slice query_slize = Slice(context->query_mbr);
                query_mbr_ = ReadQueryMbr(query_slize);
            }
        }

        ~Iterator() override {}

        bool Valid() const override { 
            return !iter_.IsNull(); 
        }

        const char* key() const override;

        void Next() override { 
            // rtree_i->GetNext(iter_);
            
            // std::cout << "Next()" << std::endl;
            
            ++iter_;
            // bool rrrr = ++iter_; 
            // std::cout << "++iter_: " << rrrr << std::endl;

            // std::cout << "it->key(): " <<  ReadKeyMbr(ExtractUserKey(GetLengthPrefixedSlice(key()))) << std::endl;
            NextIfDisjoint(); 
            // Slice key_inter = GetLengthPrefixedSlice(key());
            // Slice key_s = ExtractUserKey(key_inter);
            // Mbr key_mbr = ReadKeyMbr(key_s);
            // std::cout << "mbr after Rtreerep next: " << key_mbr << std::endl;
        }

        void Prev() override {} // not implemented for r-tree

        void Seek(const Slice& user_key, const char* memtable_key) override {
            // std::cout << "Rtreerep Seek" << std::endl;
            (void) user_key;
            (void) memtable_key;
        } // not implemented for r-tree

        void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
            (void) user_key;
            (void) memtable_key;
        } // not implemented for r-tree

        void RandomSeek() override {} // not implemented yet for r-tree

        void SeekToFirst() override { 
            rtree_i->GetFirst(iter_); 
            NextIfDisjoint();

            // debug
            // Slice key_inter = GetLengthPrefixedSlice(key());
            // Slice key_s = ExtractUserKey(key_inter);
            // Mbr key_mbr = ReadKeyMbr(key_s);
            // std::cout << "mbr after Rtreerep seektofirst: " << key_mbr << std::endl;

        }
        
        void SeekToLast() override {} // not implemented yet for r-tree

     private:
        MyTree::Iterator iter_;
        MyTree* rtree_i;
        Mbr query_mbr_;
        void NextIfDisjoint() {
            
            if (Valid()) {
                Slice key_inter = GetLengthPrefixedSlice(key());
                Slice key_s = ExtractUserKey(key_inter);
                Mbr key_mbr = ReadKeyMbr(key_s);

                // Mbr key_mbr;

                // int boundsMin[2] = {0,0};
                // int boundsMax[2] = {0,0};
                // iter_.GetBounds(boundsMin, boundsMax);
                // key_mbr.set_iid(boundsMin[0], boundsMin[1]);
                // key_mbr.set_first(boundsMax[0], boundsMax[1]);
                // key_mbr.set_second(boundsMax[0], boundsMax[1]);
                
                // //debug
                // std::cout << "disjoint check: " << IntersectMbr(key_mbr, query_mbr_) << std::endl;
                // std::cout << "key mbr: " << key_mbr << std::endl;
                // std::cout << "query mbr: " << query_mbr_ << std::endl;

                while (!IntersectMbr(key_mbr, query_mbr_)){
                    
                    std::cout << "!IntersectMbr => Next()" << std::endl;
                    // Next();
                    ++iter_;

                    key_inter = GetLengthPrefixedSlice(key());
                    key_s = ExtractUserKey(key_inter);
                    key_mbr = ReadKeyMbr(key_s);
                } 

            
            
            }

        }

    };

    // Returns an iterator over the keys
    MemTableRep::Iterator* GetIterator(IteratorContext* iterator_context, Arena* arena = nullptr) override;


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

bool MySearchCallback(char* id)
{
    (void) id;
    return true; // keep going
}


bool RtreeRep::Contains(const char* key) const {
    Slice internal_key = GetLengthPrefixedSlice(key);
    Slice user_key = ExtractUserKey(internal_key);
    Mbr key_mbr = ReadKeyMbr(user_key);
    Rect key_rect(key_mbr.first.min, key_mbr.second.min, key_mbr.first.max, key_mbr.second.max);

    std::vector<char*> nhits;
    nhits = rtree_.Search(key_rect.min, key_rect.max, MySearchCallback);
    
    if (nhits.size() > 0) {
        return true;
    } else {
        return false;
    }

}

const char* RtreeRep::Iterator::key() const {
    
    // int key_iid = rtree_i->GetAt(iter_);

    // int boundsMin[2] = {0,0};
    // int boundsMax[2] = {0,0};
    // iter_.GetBounds(boundsMin, boundsMax);

    // //debug
    // std::cout << "key()" << std::endl;
    // std::cout << "Bounds :" << boundsMin[0] << " , " << boundsMax[0] << std::endl;
    // std::cout << "key: " << iter_.operator*() << std::endl;
    // //debug

    // const char* key = serialize_key(boundsMin[0], boundsMax[0]).c_str();
    return iter_.operator*();

    // return key;
}

void RtreeRep::Get(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) {
        (void) k;
        (void) callback_args;
        (void) callback_func;
    }

void RtreeRep::SpatialRange(const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry)) {
        
        (void) k;
        (void) callback_args;
        (void) callback_func;
    }

MemTableRep::Iterator* RtreeRep::GetIterator(IteratorContext* iterator_context, Arena* arena) { 
    void *mem = arena ? arena->AllocateAligned(sizeof(RtreeRep::Iterator))
    : operator new(sizeof(RtreeRep::Iterator));
    return new (mem) RtreeRep::Iterator(&rtree_, iterator_context);
}

}

MemTableRep* RTreeFactory::CreateMemTableRep(
        const MemTableRep::KeyComparator&, Allocator* allocator,
        const SliceTransform*, Logger* /*logger*/) {
    // std::cout << "new memtable" << std::endl;
    return new RtreeRep(allocator);
}



} //namespace ROCKSDB_NAMESPACE
