#include <iostream>
#include <fstream>
#include <string>

#include "InputParser.h"
#include "TripleStore.h"
#include "DatalogEngine.h"
#include "Trie.h" // 测试用，暂时包含Trie.h，后续删除

//// 测试用，打印文件内容
void printFileContent(const std::string& filename) {
    std::ifstream file(filename);
    std::string line;
    while (std::getline(file, line)) {
        std::cout << line << std::endl;
    }
}

//// 测试用，解析并打印n-triples文件
void parseNTFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseNTriples(filename);
    for (const auto& triple : triples) {
        std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    }
}

//// 测试用，解析并打印turtle文件
void parseTurtleFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseTurtle(filename);
    for (const auto& triple : triples) {
        std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    }
}

//// 测试用，解析并打印csv文件
void parseCSVFile(const std::string& filename) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseCSV(filename);
    for (const auto& triple : triples) {
        std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    }
}

//// 测试用，测试TripleStore的按主语查询功能
void TestQueryBySubject() {
    InputParser parser;
    TripleStore store;

    std::vector<Triple> triples = parser.parseTurtle("input_examples/example.ttl");
    for (const auto& triple : triples) {
        store.addTriple(triple);
    }

    std::vector<Triple> queryResult = store.queryBySubject("http://example.org/Alice");
    for (const auto& triple : queryResult) {
        std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    }
}

//// 测试用，测试简单推理功能
void TestInfer() {
    InputParser parser;
    TripleStore store;

    std::vector<Triple> triples = parser.parseTurtle("input_examples/example.ttl");
    for (const auto& triple : triples) {
        store.addTriple(triple);
    }

    // 定义规则
    std::vector<Rule> rules;
    Rule rule1(
            "rule1",
            std::vector<Triple>{
                {"?x", "http://example.org/friendOf", "?y"},
            },
            Triple{"?x", "http://example.org/knows", "?y"}
    );

    Rule rule2(
            "rule2",
            std::vector<Triple>{
                // {"?x", "http://example.org/knows", "?y"},
                {"?x", "http://example.org/friendOf", "?y"},
                {"?y", "http://example.org/friendOf", "?z"},
            },
            Triple{"?x", "http://example.org/knows", "?z"}
    );

    rules.push_back(rule1);
    // rules.push_back(rule2);

    // 创建DatalogEngine实例
    DatalogEngine engine(store, rules);

    // 执行推理
    engine.reason();

    // 查询推理结果
    std::vector<Triple> queryResult = store.queryByPredicate("http://example.org/knows");
    for (const auto& triple : queryResult) {
        std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
    }
}

//// 测试用，解析并打印Datalog文件
void TestDatalogParser() {
    InputParser parser;
    std::vector<Rule> rules = parser.parseDatalogFromFile("input_examples/ruleExample.dl");
    for (const auto& rule : rules) {
        std::cout << rule.name << std::endl;
        for (const auto& triple : rule.body) {
            std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
        }
        std::cout << "=> " << rule.head.subject << " " << rule.head.predicate << " " << rule.head.object << std::endl;
    }
}

//// 测试用，使用TrieIterator遍历Trie
void TestTrieIterator() {
    Trie trie;

    // 插入三元组
    trie.insert(Triple("Alice", "knows", "Bob"));
    trie.insert(Triple("Alice", "likes", "Chocolate"));
    trie.insert(Triple("Bob", "knows", "Charlie"));

    // 创建 TrieIterator
    TrieIterator it(trie.root);

    // 遍历 Trie
    while (!it.atEnd()) {
        std::cout << "Predicate: " << it.key() << std::endl;
        TrieIterator subjectIt = it.open();
        while (!subjectIt.atEnd()) {
            std::cout << "  Subject: " << subjectIt.key() << std::endl;
            TrieIterator objectIt = subjectIt.open();
            while (!objectIt.atEnd()) {
                std::cout << "    Object: " << objectIt.key() << std::endl;
                objectIt.next();
            }
            subjectIt.next();
        }
        it.next();
    }

}

int main() {

    // TestInfer();
    // TestDatalogParser();
    TestTrieIterator();

    return 0;
}
