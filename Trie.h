#ifndef RDFPANDA_STORAGE_TRIE_H
#define RDFPANDA_STORAGE_TRIE_H


#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iostream>

// Triple 和 Rule 类定义
class Triple {
public:
    std::string subject;
    std::string predicate;
    std::string object;

    Triple(std::string subject, std::string predicate, std::string object)
            : subject(std::move(subject)), predicate(std::move(predicate)), object(std::move(object)) {}

    bool operator<(const Triple& rhs) const {
        if (subject != rhs.subject)
            return subject < rhs.subject;
        if (predicate != rhs.predicate)
            return predicate < rhs.predicate;
        return object < rhs.object;
    }

    bool operator==(const Triple& rhs) const {
        return subject == rhs.subject && predicate == rhs.predicate && object == rhs.object;
    }

    bool operator!=(const Triple& rhs) const {
        return !(*this == rhs);
    }
};

class Rule {
public:
    std::string name;
    std::vector<Triple> body;
    Triple head;

    Rule(std::string name, const std::vector<Triple>& body, Triple head)
            : name(std::move(name)), body(body), head(std::move(head)) {}
};

// TrieNode：Trie 的节点，使用 std::map 保持子节点有序（即 PSO 顺序中的字典顺序）
class TrieNode {
public:
    std::map<std::string, TrieNode*> children;
    bool isEnd;

    TrieNode() : isEnd(false) {}
    ~TrieNode() {
        for (auto& pair : children) {
            delete pair.second;
        }
    }
};

// Trie 类，按 PSO 顺序存储三元组
// update: 按 PSO 和 POS 两种顺序存储三元组
class Trie {
public:
    TrieNode* root;

    Trie() {
        root = new TrieNode();
    }
    ~Trie() {
        delete root;
    }

    void insertPSO(const Triple& triple);
    void insertPOS(const Triple& triple);
    void deletePSO(const Triple& triple);
    void deletePOS(const Triple& triple);
    void printAll();

private:
    void printAllHelper(TrieNode* node, std::vector<std::string>& binding);

};

// TrieIterator：对 TrieNode 的子节点进行遍历，提供类似迭代器的接口
class TrieIterator {
public:
    TrieNode* node; // 当前所在节点
    std::map<std::string, TrieNode*>::iterator it;
    std::map<std::string, TrieNode*>::iterator end;

    TrieIterator(TrieNode* n) : node(n) {
        if (node) {
            it = node->children.begin();
            end = node->children.end();
        }
    }

    bool atEnd() const {
        return it == end;
    }

    std::string key() const {
        return it->first;
    }

    void next() {
        if (!atEnd()) {
            ++it;
        }
    }

    // 跳跃到不小于 target 的位置
    void seek(const std::string& target) {
        it = node->children.lower_bound(target);
    }

    // open()：进入当前 key 对应的子节点，返回新的 TrieIterator
    TrieIterator open() {
        if (!atEnd()) {
            return TrieIterator(it->second);
        }
        return TrieIterator(nullptr);
    }
};

// LeapfrogJoin类：在一组TrieIterator上实现leapfrog交集查找（适用于单变量join）
class LeapfrogJoin {
public:
    std::vector<TrieIterator*> iterators;
    int p;       // 当前指针索引
    bool done;   // 标记是否结束

    LeapfrogJoin(std::vector<TrieIterator*>& its) : iterators(its), p(0), done(false) {
        // 对所有迭代器按当前 key 从小到大排序
        std::sort(iterators.begin(), iterators.end(), [](TrieIterator* a, TrieIterator* b) {
            return a->key() < b->key();
        });
        leapfrog_search();
    }

    bool atEnd() const {
        return done;
    }

    std::string key() const {
        return iterators[p]->key();
    }

    TrieIterator open() {
        return iterators[p]->open();
    }

    void next() {
        iterators[p]->next();
        if (iterators[p]->atEnd()) {
            done = true;
            return;
        }
        p = (p + 1) % iterators.size();
        leapfrog_search();
    }

private:
    void leapfrog_search();

};


#endif //RDFPANDA_STORAGE_TRIE_H
