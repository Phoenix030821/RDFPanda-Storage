#include "../InputParser.h"
#include "gtest/gtest.h"

TEST(InputParserTest, ParseNTriples) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseNTriples("input_examples/example.nt");

    ASSERT_EQ(triples.size(), 3);
    EXPECT_EQ(std::get<0>(triples[0]), "http://example.org/subject");
    EXPECT_EQ(std::get<1>(triples[0]), "http://example.org/predicate");
    EXPECT_EQ(std::get<2>(triples[0]), "\"object\"");
}

TEST(InputParserTest, ParseTurtle) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseTurtle("input_examples/example.ttl");

    ASSERT_EQ(triples.size(), 3);
    EXPECT_EQ(std::get<0>(triples[0]), "http://example.org/subject");
    EXPECT_EQ(std::get<1>(triples[0]), "http://example.org/predicate");
    EXPECT_EQ(std::get<2>(triples[0]), "\"object\"");
}

TEST(InputParserTest, ParseCSV) {
    InputParser parser;
    std::vector<Triple> triples = parser.parseCSV("input_examples/example.csv");

    ASSERT_EQ(triples.size(), 3);
    EXPECT_EQ(std::get<0>(triples[0]), "subject1");
    EXPECT_EQ(std::get<1>(triples[0]), "predicate1");
    EXPECT_EQ(std::get<2>(triples[0]), "object1");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}