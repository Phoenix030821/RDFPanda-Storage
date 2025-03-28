#include "Trie.h"

// 插入时采用 PSO 顺序：先插入 predicate，再 subject，最后 object
void Trie::insertPSO(const Triple& triple) {
    TrieNode* curr = root;
    // 顺序：predicate, subject, object
    std::vector<std::string> keys = { triple.predicate, triple.subject, triple.object };
    for (const auto & key : keys) {
        if (curr->children.find(key) == curr->children.end()) {
            curr->children[key] = new TrieNode();
        }
        curr = curr->children[key];
    }
    curr->isEnd = true;
}

// 插入时采用 POS 顺序：先插入 predicate，再 object，最后 subject
void Trie::insertPOS(const Triple &triple) {
    TrieNode* curr = root;
    // 顺序：predicate, object, subject
    std::vector<std::string> keys = { triple.predicate, triple.object, triple.subject };
    for (const auto & key : keys) {
        if (curr->children.find(key) == curr->children.end()) {
            curr->children[key] = new TrieNode();
        }
        curr = curr->children[key];
    }
    curr->isEnd = true;
}


// 仅用于调试，遍历并打印 Trie 中所有存储的三元组
void Trie::printAll() {
    std::vector<std::string> binding;
    printAllHelper(root, binding);
}

void Trie::printAllHelper(TrieNode* node, std::vector<std::string>& binding) {
    if (binding.size() == 3 && node->isEnd) {
        // binding 中顺序为 [predicate, subject, object]，
        // 但 Triple 构造函数要求 (subject, predicate, object)
        std::cout << "Triple: (" << binding[1] << ", " << binding[0] << ", " << binding[2] << ")\n";
    }
    for (auto& pair : node->children) {
        binding.push_back(pair.first);
        printAllHelper(pair.second, binding);
        binding.pop_back();
    }
}

void LeapfrogJoin::leapfrog_search() {
    if (iterators.empty()) {
        done = true;
        return;
    }
    while (true) {
        // 找出所有迭代器中最大的当前 key
        std::string maxKey = iterators[0]->key();
        for (auto it : iterators) {
            if (it->key() > maxKey)
                maxKey = it->key();
        }
        // 对于当前 key 小于 maxKey 的迭代器，执行 seek(maxKey)
        bool allEqual = true;
        for (TrieIterator* it : iterators) {
            if (it->key() < maxKey) {
                it->seek(maxKey);
                if (it->atEnd()) {
                    done = true;
                    return;
                }
                allEqual = false;
            }
        }
        if (allEqual) break;
    }
}