#include <chrono>
#include <iostream>
#include <fstream>
#include <string>

#include "InputParser.h"
#include "TripleStore.h"
#include "DatalogEngine.h"

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
                {"?x", "http://example.org/knows", "?y"},
                {"?y", "http://example.org/knows", "?z"},
            },
            Triple{"?x", "http://example.org/knows", "?z"}
    );

    rules.push_back(rule1);  // 一次迭代
    rules.push_back(rule2);  // 两次迭代，需要用到rule1的推理结果

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
    // std::vector<Rule> rules = parser.parseDatalogFromFile("input_examples/ruleExample.dl");
    std::vector<Rule> rules = parser.parseDatalogFromFile("input_examples/DAG-R.dl");
    for (const auto& rule : rules) {
        std::cout << rule.name << std::endl;
        for (const auto& triple : rule.body) {
            std::cout << triple.subject << " " << triple.predicate << " " << triple.object << std::endl;
        }
        std::cout << "=> " << rule.head.subject << " " << rule.head.predicate << " " << rule.head.object << std::endl;
    }
}

//// 测试大文件读入及推理
void TestLargeFile() {
    InputParser parser;
    TripleStore store;
    std::vector<Triple> triples = parser.parseTurtle("input_examples/DAG.ttl");
    // std::cout << "Total triples: " << triples.size() << std::endl;
    int count = 0;
    for (const auto& triple : triples) {
        // std::cout << count++ << std::endl;
        store.addTriple(triple);
    }

    std::vector<Rule> rules = parser.parseDatalogFromFile("input_examples/DAG-R.dl");

    DatalogEngine engine(store, rules);
    engine.reason();

}

//// 测试百到万级三元组和两位数规则
void TestMidFile() {
    InputParser parser;
    TripleStore store;
    std::vector<Triple> triples = parser.parseTurtle("input_examples/mid-k.ttl");
    // std::cout << "Total triples: " << triples.size() << std::endl;
    int count = 0;
    for (const auto& triple : triples) {
        // std::cout << count++ << std::endl;
        store.addTriple(triple);
    }

    std::vector<Rule> rules = parser.parseDatalogFromFile("input_examples/mid.dl");

    DatalogEngine engine(store, rules);
    engine.reason();

}

//// 计时用
void startTimer() {
    // 用结束时间与开始时间相减
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    // 这里调用要测试的函数
    // TestLargeFile();
    TestMidFile();

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    // 输出用时
    std::cout << "Elapsed time: " << elapsed.count() << " seconds" << std::endl;
}

int main() {

    // TestInfer();
    // TestDatalogParser();
    // TestLargeFile();
    startTimer();

    return 0;
}
