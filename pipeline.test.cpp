#include "./pipeline.h"
#include <cstddef>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>
#include <sstream>
#include <string>

#include <catch2/catch.hpp>

template <typename Output>
struct simplest_source : ppl::source<Output> {
    Output val_;
    const std::string name_;

    simplest_source() = default;

    simplest_source(const std::string &name): name_{name} {};

    auto name() const -> std::string override {
        return name_;
    }

    auto poll_next() -> ppl::poll override {
        return ppl::poll::ready;
    }

    auto value() const -> const Output& override {
        return val_;
    }
};

template<typename Input, typename Output>
struct simplest_component : ppl::component<Input, Output> {
    Output val_;
    const std::string name_;

    simplest_component() = default;

    simplest_component(const std::string &name): name_{name} {};

    auto name() const -> std::string override {
        return name_;
    }

    void connect(const ppl::node*, int) override {}

    auto poll_next() -> ppl::poll override {
        return ppl::poll::ready;
    }

    auto value() const -> const Output& override {
        return val_;
    }
};

template <typename Input>
struct simplest_sink : ppl::sink<Input> {
    const std::string name_;

    simplest_sink() = default;

    simplest_sink(const std::string &name): name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    void connect(const ppl::node*, int) override {}

    auto poll_next() -> ppl::poll override {
        return ppl::poll::ready;
    }
};

TEST_CASE("Testing erase_node generates error when erasing an invalid node") {
	auto pipeline = ppl::pipeline{};
    const auto source1 = pipeline.create_node<simplest_source<int>>();
    const auto source2 = pipeline.create_node<simplest_source<char>>();
    const auto c = pipeline.create_node<simplest_component<std::tuple<int, char>, int>>();
    pipeline.connect(source1, c, 0);
    pipeline.connect(source2, c, 1);
    pipeline.erase_node(c);
    CHECK_THROWS_AS(pipeline.erase_node(c), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.erase_node(-1), ppl::pipeline_error);
}

TEST_CASE("Testing get_node") {
	auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    CHECK(pipeline.get_node(source) != nullptr);
    pipeline.erase_node(source);
    CHECK(pipeline.get_node(source) == nullptr);
}

TEST_CASE("Testing connection type match and mismatch") {
	auto pipeline = ppl::pipeline{};
    const auto source1 = pipeline.create_node<simplest_source<int>>();
    const auto source2 = pipeline.create_node<simplest_source<char>>();
    const auto source3 = pipeline.create_node<simplest_source<double>>();
    const auto source4 = pipeline.create_node<simplest_source<std::string>>();
    const auto c = pipeline.create_node<simplest_component<std::tuple<int, char, double, std::string>, int>>();

    // Check that exception is thrown when there's a mismatch
    CHECK_THROWS_AS(pipeline.connect(source1, c, 1), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source1, c, 2), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source1, c, 3), ppl::pipeline_error);

    CHECK_THROWS_AS(pipeline.connect(source2, c, 0), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source2, c, 2), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source2, c, 3), ppl::pipeline_error);

    CHECK_THROWS_AS(pipeline.connect(source3, c, 0), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source3, c, 1), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source3, c, 3), ppl::pipeline_error);

    CHECK_THROWS_AS(pipeline.connect(source4, c, 0), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source4, c, 1), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source4, c, 2), ppl::pipeline_error);

    // Check that no exception is thrown when there's a match
    CHECK_NOTHROW(pipeline.connect(source1, c, 0));
    CHECK_NOTHROW(pipeline.connect(source2, c, 1));
    CHECK_NOTHROW(pipeline.connect(source3, c, 2));
    CHECK_NOTHROW(pipeline.connect(source4, c, 3));
}

TEST_CASE("Testing connect with invalid slot") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    CHECK_THROWS_AS(pipeline.connect(source, c1, -1), ppl::pipeline_error);
    CHECK_THROWS_AS(pipeline.connect(source, c1, 2), ppl::pipeline_error);
}

TEST_CASE("Testing connect when slot is already occupied") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    CHECK_NOTHROW(pipeline.connect(source, c1, 0));
    CHECK_THROWS_AS(pipeline.connect(c2, c1, 2), ppl::pipeline_error);
}

TEST_CASE("Testing connect with invalid handle") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.erase_node(source);
    CHECK_THROWS_AS(pipeline.connect(source, c1, 0), ppl::pipeline_error);
}

TEST_CASE("Testing disconnect with invalid handle") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.erase_node(source);
    CHECK_THROWS_AS(pipeline.disconnect(source, c1), ppl::pipeline_error);
}

TEST_CASE("Testing get dependencies with invalid handle") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    pipeline.erase_node(source);
    CHECK_THROWS_AS(pipeline.get_dependencies(source), ppl::pipeline_error);
}

TEST_CASE("Testing get_dependencies with multiple dependencies for one source") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c3 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    pipeline.connect(source, c2, 0);
    pipeline.connect(source, c3, 0);
    auto dependencies = pipeline.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c2, 0}, {c3, 0}});
    CHECK(pipeline.get_dependencies(c1) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
    CHECK(pipeline.get_dependencies(c2) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
}

TEST_CASE("Testing get_dependencies on a sink") {
    auto pipeline = ppl::pipeline{};
    const auto sink = pipeline.create_node<simplest_sink<int>>();
    CHECK(pipeline.get_dependencies(sink) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
}

TEST_CASE("Testing get_dependencies with same sources to one destination") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int, int>, int>>();
    pipeline.connect(source, c1, 0);
    pipeline.connect(source, c1, 1);
    auto dependencies = pipeline.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c1, 1}});
}

TEST_CASE("Testing when a node has been erased and the result from get_dependencies") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    pipeline.connect(source, c2, 0);
    auto dependencies = pipeline.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c2,0}});
    pipeline.erase_node(c1);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c2,0}});
    pipeline.erase_node(c2);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
}

TEST_CASE("Testing when a node has been disconnected and the result from get_dependencies") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c3 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    pipeline.connect(source, c1, 1);
    pipeline.connect(source, c2, 0);
    auto dependencies = pipeline.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c1, 1}, {c2, 0}});
    pipeline.disconnect(source, c1);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c2, 0}});
    pipeline.disconnect(source, c2);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});

    // If nodes are not connected, nothing happens
    CHECK_NOTHROW(pipeline.disconnect(source, c1));
    CHECK_NOTHROW(pipeline.disconnect(source, c2));
    CHECK_NOTHROW(pipeline.disconnect(source, c3));
}

TEST_CASE("Testing when a node has been disconnected and another node claims its previous slot") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}});
    CHECK_THROWS_AS(pipeline.connect(c2, c1, 0), ppl::pipeline_error);
    pipeline.disconnect(source, c1);
    CHECK_NOTHROW(pipeline.connect(c2, c1, 0));
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
    CHECK(pipeline.get_dependencies(c2) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}});
}

TEST_CASE("Testing when a node has been erased and another node claims its previous slot") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    CHECK(pipeline.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}});
    CHECK_THROWS_AS(pipeline.connect(c2, c1, 0), ppl::pipeline_error);
    pipeline.erase_node(source);
    CHECK_NOTHROW(pipeline.connect(c2, c1, 0));
    CHECK(pipeline.get_dependencies(c2) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}});
}

TEST_CASE("Testing is_valid for 'All source slots for all nodes must be filled.'") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();

    // slot 1 for c1 has not been filled
    pipeline.connect(source, c1, 0);
    pipeline.connect(c1, sink, 0);
    CHECK(!pipeline.is_valid());

    // Slot 1 for c1 is now filled
    pipeline.connect(source, c1, 1);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'All non-sink nodes must have at least one dependent.'") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();

    // c2 does not have any dependents
    pipeline.connect(source, c1, 0);
    pipeline.connect(source, c2, 0);
    pipeline.connect(c1, sink, 0);
    CHECK(!pipeline.is_valid());

    // Connect c2 to another sink
    const auto sink2 = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(c2, sink2, 0);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There is at least 1 source node.'") {
    auto pipeline = ppl::pipeline{};
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(c1, sink, 0);
    CHECK(!pipeline.is_valid());
    const auto source = pipeline.create_node<simplest_source<int>>();
    pipeline.connect(source, c1, 0);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There is at least 1 sink node.'") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    pipeline.connect(source, c1, 0);
    CHECK(!pipeline.is_valid());
    const auto sink = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(c1, sink, 0);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There are no subpipelines i.e. completely disconnected sections of the dataflow from the main pipeline.'") {
    auto pipeline = ppl::pipeline{};
    // First subpipeline: source1 -> c1 -> sink1
    const auto source1 = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink1 = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(source1, c1, 0);
    pipeline.connect(c1, sink1, 0);
    CHECK(pipeline.is_valid());

    // Second subpipeline: source2 -> c2 -> sink2
    const auto source2 = pipeline.create_node<simplest_source<int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink2 = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(source2, c2, 0);
    pipeline.connect(c2, sink2, 0);
    CHECK(!pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There are no cycles.' between 2 nodes") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();

    // Cycle between c1, c2
    pipeline.connect(source, c1, 0);
    pipeline.connect(c1, c2, 0);
    pipeline.connect(c2, c1, 1);
    pipeline.connect(c1, sink, 0);
    CHECK(!pipeline.is_valid());

    // Remove edge from c2 to c1 to cease cycle
    pipeline.disconnect(c2, c1);

    // Fill empty slot in c1
    pipeline.connect(source, c1, 1);

    // Give c2 a dependent sink
    const auto sink2 = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(c2, sink2, 0);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There are no cycles.' between 3 nodes") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto c3 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();

    // Cycle amongst c1, c2, c3
    pipeline.connect(source, c1, 0);
    pipeline.connect(c1, c2, 0);
    pipeline.connect(c2, c3, 0);
    pipeline.connect(c3, c1, 1);
    pipeline.connect(c3, sink, 0);
    CHECK(!pipeline.is_valid());

    // Remove edge from c3 to c1 to cease cycle
    pipeline.disconnect(c3, c1);

    // Fill empty slot in c1
    pipeline.connect(source, c1, 1);
    CHECK(pipeline.is_valid());
}

TEST_CASE("Testing is_valid for 'There are no cycles.' when a node is connected to itself") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto sink = pipeline.create_node<simplest_sink<int>>();
    pipeline.connect(source, c1, 0);
    pipeline.connect(c1, sink, 0);
    pipeline.connect(c1, c1, 1);
    CHECK(!pipeline.is_valid());
}

TEST_CASE("Testing visual representation: simple") {
    auto pipeline = ppl::pipeline{};
    const auto hello = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>("hello");
    const auto world = pipeline.create_node<simplest_source<int>>("world");
    const auto deleted = pipeline.create_node<simplest_source<int>>("deleted");
    const auto foobar = pipeline.create_node<simplest_component<std::tuple<int>, int>>("foobar");
    pipeline.erase_node(deleted);
    pipeline.connect(world, hello, 0);
    pipeline.connect(world, foobar, 0);
    pipeline.connect(foobar, hello, 1);
    std::stringstream ss;
    ss << pipeline;
    CHECK(ss.str() == "digraph G {\n  \"1 hello\"\n  \"2 world\"\n  \"4 foobar\"\n\n"
    "  \"2 world\" -> \"1 hello\"\n  \"2 world\" -> \"4 foobar\"\n  \"4 foobar\" -> \"1 hello\"\n}\n");
}

TEST_CASE("Testing visual representation: with multiple paths between same pair of nodes") {
    auto pipeline = ppl::pipeline{};
    const auto hello = pipeline.create_node<simplest_component<std::tuple<int, int, int>, int>>("hello");
    const auto world = pipeline.create_node<simplest_source<int>>("world");
    const auto deleted = pipeline.create_node<simplest_source<int>>("deleted");
    const auto foobar = pipeline.create_node<simplest_component<std::tuple<int>, int>>("foobar");
    pipeline.erase_node(deleted);
    pipeline.connect(world, hello, 0);
    pipeline.connect(world, foobar, 0);
    pipeline.connect(foobar, hello, 1);
    pipeline.connect(foobar, hello, 2);
    std::stringstream ss;
    ss << pipeline;
    CHECK(ss.str() == "digraph G {\n  \"1 hello\"\n  \"2 world\"\n  \"4 foobar\"\n\n  \"2 world\""
    " -> \"1 hello\"\n  \"2 world\" -> \"4 foobar\"\n  \"4 foobar\" -> \"1 hello\"\n  \"4 foobar\" -> \"1 hello\"\n}\n");
}

TEST_CASE("Testing visual representation: more than 10 nodes") {
    auto pipeline = ppl::pipeline{};
    const auto one = pipeline.create_node<simplest_component<std::tuple<int>, int>>("one");
    const auto two = pipeline.create_node<simplest_source<int>>("two");
    pipeline.create_node<simplest_source<int>>("three");
    pipeline.create_node<simplest_source<int>>("four");
    pipeline.create_node<simplest_source<int>>("five");
    pipeline.create_node<simplest_source<int>>("six");
    pipeline.create_node<simplest_source<int>>("seven");
    pipeline.create_node<simplest_source<int>>("eight");
    pipeline.create_node<simplest_source<int>>("nine");
    pipeline.create_node<simplest_source<int>>("ten");
    const auto eleven = pipeline.create_node<simplest_component<std::tuple<int>, int>>("eleven");
    const auto twelve = pipeline.create_node<simplest_source<int>>("twelve");
    pipeline.connect(twelve, one, 0);
    pipeline.connect(two, eleven, 0);
    std::stringstream ss;
    ss << pipeline;
    CHECK(ss.str() == "digraph G {\n  \"1 one\"\n  \"2 two\"\n  \"3 three\"\n"
        "  \"4 four\"\n  \"5 five\"\n  \"6 six\"\n  \"7 seven\"\n  \"8 eight\"\n  \"9 nine\"\n"
        "  \"10 ten\"\n  \"11 eleven\"\n  \"12 twelve\"\n\n  \"2 two\""
        " -> \"11 eleven\"\n  \"12 twelve\" -> \"1 one\"\n}\n");
}

TEST_CASE("Testing the move constructor for pipeline") {
    auto pipeline = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    auto moved = std::move(pipeline);
    CHECK_NOTHROW(moved.connect(source, c1, 0));
    CHECK_NOTHROW(moved.connect(source, c1, 1));
    CHECK_NOTHROW(moved.connect(source, c2, 0));
    auto dependencies = moved.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c1, 1}, {c2, 0}});
    CHECK_NOTHROW(moved.disconnect(source, c1));
    CHECK(moved.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c2, 0}});
    CHECK_NOTHROW(moved.disconnect(source, c2));
    CHECK(moved.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
}

TEST_CASE("Testing the move assignment for pipeline") {
    auto pipeline = ppl::pipeline{};
    auto moved = ppl::pipeline{};
    const auto source = pipeline.create_node<simplest_source<int>>();
    const auto c1 = pipeline.create_node<simplest_component<std::tuple<int, int>, int>>();
    const auto c2 = pipeline.create_node<simplest_component<std::tuple<int>, int>>();
    moved = std::move(pipeline);
    CHECK_NOTHROW(moved.connect(source, c1, 0));
    CHECK_NOTHROW(moved.connect(source, c1, 1));
    CHECK_NOTHROW(moved.connect(source, c2, 0));
    auto dependencies = moved.get_dependencies(source);
    std::sort(dependencies.begin(), dependencies.end());
    CHECK(dependencies == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c1, 0}, {c1, 1}, {c2, 0}});
    CHECK_NOTHROW(moved.disconnect(source, c1));
    CHECK(moved.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{{c2, 0}});
    CHECK_NOTHROW(moved.disconnect(source, c2));
    CHECK(moved.get_dependencies(source) == std::vector<std::pair<ppl::pipeline::node_id, int>>{});
}

// This vector is used to keep track of which nodes have been polled
auto polled = std::vector<std::string>{};

struct int_source : ppl::source<int> {
    int val_;
    const std::string name_;

    int_source(const int &val, const std::string &name): val_{val}, name_{name} {};

    auto name() const -> std::string override {
        return name_;
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        val_++;
        if (val_ >= 5) {
            return ppl::poll::closed;
        }
        if (val_ <= 0) {
            return ppl::poll::empty;
        }
        return ppl::poll::ready;
    }

    auto value() const -> const int& override {
        return val_;
    }
};

struct string_source : ppl::source<std::string> {
    std::string val_;
    const std::string name_;

    string_source(const std::string &val, const std::string &name): val_{val}, name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        if (val_.size() >= 3) {
            return ppl::poll::closed;
        }
        val_ += "a";
        return ppl::poll::ready;
    }

    auto value() const -> const std::string& override {
        return val_;
    }
};

struct string_component : ppl::component<std::tuple<std::string>, std::string> {
    std::string val_;
    const std::string name_;
    const ppl::producer<std::string>* slot0 = nullptr;

    string_component(const std::string &name): name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    auto connect(const ppl::node* src, int slot) -> void override {
        if (slot == 0) {
            slot0 = dynamic_cast<const ppl::producer<std::string>*>(src);
        }
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        if (slot0 != nullptr) {
            val_ = slot0->value();
            return ppl::poll::ready;
        }
        return ppl::poll::empty;
    }

    auto value() const -> const std::string& override {
        return val_;
    }
};

struct int_component : ppl::component<std::tuple<int>, int> {
    int val_;
    const std::string name_;
    const ppl::producer<int>* slot0 = nullptr;

    int_component(const std::string &name): name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    auto connect(const ppl::node* src, int slot) -> void override {
        if (slot == 0) {
            slot0 = dynamic_cast<const ppl::producer<int>*>(src);
        }
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        if (slot0 != nullptr) {
            val_ = slot0->value();
            return ppl::poll::ready;
        }
        // std::cout << "hello\n";
        return ppl::poll::empty;
    }

    auto value() const -> const int& override {
        return val_;
    }
};

struct mixed_component : ppl::component<std::tuple<int, std::string>, std::string> {
    std::string val_;
    const std::string name_;
    const ppl::producer<int>* slot0 = nullptr;
    const ppl::producer<std::string>* slot1 = nullptr;

    mixed_component(const std::string &name): name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    auto connect(const ppl::node* src, int slot) -> void override {
        if (slot == 0) {
            slot0 = dynamic_cast<const ppl::producer<int>*>(src);
        } else if (slot == 1) {
            slot1 = dynamic_cast<const ppl::producer<std::string>*>(src);
        }
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        if (slot0 != nullptr && slot1 != nullptr) {
            val_ = std::to_string(slot0->value()) + " " + slot1->value();
            return ppl::poll::ready;
        }
        return ppl::poll::empty;
    }

    auto value() const -> const std::string& override {
        return val_;
    }
};

template <typename Input>
struct simple_sink : ppl::sink<Input> {
    const std::string name_;
    const ppl::producer<Input>* slot0 = nullptr;

    simple_sink(const std::string &name): name_{name} {}

    auto name() const -> std::string override {
        return name_;
    }

    void connect(const ppl::node* src, int slot) override {
        if (slot == 0) {
            slot0 = dynamic_cast<const ppl::producer<Input>*>(src);;
        }
    }

    auto poll_next() -> ppl::poll override {
        polled.push_back(name_);
        if (slot0 == nullptr) {
            return ppl::poll::empty;
        }
        return ppl::poll::ready;
    }

public:
    auto outcome() const -> Input {
        return slot0->value();
    }
};

TEST_CASE("Testing step for a single sink") {
    polled.clear();
    auto pipeline = ppl::pipeline{};       
    const auto source1 = pipeline.create_node<int_source>(0, "source1");
    const auto source2 = pipeline.create_node<string_source>("", "source2");
    const auto c1 = pipeline.create_node<mixed_component>("c1");
    const auto sink = pipeline.create_node<simple_sink<std::string>>("sink");
    pipeline.connect(source1, c1, 0);
    pipeline.connect(source2, c1, 1);
    pipeline.connect(c1, sink, 0);

    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "1 a");
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "2 aa");
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "3 aaa");

    // Step should now return false as the only sink is closed
    CHECK(pipeline.step());
    polled.clear();
}

TEST_CASE("Testing step for multiple sinks") {
    auto pipeline = ppl::pipeline{};
    const auto source1 = pipeline.create_node<int_source>(0, "source1");
    const auto source2 = pipeline.create_node<string_source>("", "source2");
    const auto c1 = pipeline.create_node<mixed_component>("c1");
    const auto sink1 = pipeline.create_node<simple_sink<std::string>>("sink1");
    const auto sink2 = pipeline.create_node<simple_sink<int>>("sink2");
    pipeline.connect(source1, c1, 0);
    pipeline.connect(source2, c1, 1);
    pipeline.connect(c1, sink1, 0);
    pipeline.connect(source1, sink2, 0);
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink1", "sink2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink1))->outcome() == "1 a");
    CHECK(static_cast<simple_sink<int>*>(pipeline.get_node(sink2))->outcome() == 1);
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink1", "sink2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink1))->outcome() == "2 aa");
    CHECK(static_cast<simple_sink<int>*>(pipeline.get_node(sink2))->outcome() == 2);
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "sink1", "sink2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink1))->outcome() == "3 aaa");
    CHECK(static_cast<simple_sink<int>*>(pipeline.get_node(sink2))->outcome() == 3);

    // By now, sink2 should be closed
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"sink2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink1))->outcome() == "3 aaa");
    CHECK(static_cast<simple_sink<int>*>(pipeline.get_node(sink2))->outcome() == 4);
    CHECK(pipeline.step());
    polled.clear();
}

TEST_CASE("Testing that all dependent nodes on an empty node are skipped until the node is ready") {
    auto pipeline = ppl::pipeline{};
    const auto source1 = pipeline.create_node<int_source>(-2, "source1");
    const auto source2 = pipeline.create_node<string_source>("", "source2");
    const auto c1 = pipeline.create_node<int_component>("c1");
    const auto c2 = pipeline.create_node<string_component>("c2");
    const auto c3 = pipeline.create_node<mixed_component>("c3");
    const auto sink = pipeline.create_node<simple_sink<std::string>>("sink");
    pipeline.connect(source1, c1, 0);
    pipeline.connect(source2, c2, 0);
    pipeline.connect(c1, c3, 0);
    pipeline.connect(c2, c3, 1);
    pipeline.connect(c3, sink, 0);

    // Check that c1, c3, sink are skipped since they all rely on source1 which is empty for 2 steps
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "");
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c2", "source1", "source2"});
    polled.clear();
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "");
    
    // Since source1 is no longer empty, its dependencies can get polled
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "c2", "c3", "sink", "source1", "source2"});
    polled.clear();
    // Uses the current value of source2. Previous values of source2 are discarded
    CHECK(static_cast<simple_sink<std::string>*>(pipeline.get_node(sink))->outcome() == "1 aaa");

    // source2 is closed so c2, c3 and sink are closed
    CHECK(pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c1", "source1", "source2"});
    polled.clear();
}

TEST_CASE("Testing that dependent nodes can be reopened if closed source was replaced") {
    auto pipeline = ppl::pipeline{};
    // source1 is closed
    const auto source = pipeline.create_node<int_source>(6, "source");
    const auto c = pipeline.create_node<int_component>("c");
    const auto sink = pipeline.create_node<simple_sink<int>>("sink");
    pipeline.connect(source, c, 0);
    pipeline.connect(c, sink, 0); 

    CHECK(pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"source"});
    polled.clear();
    
    pipeline.erase_node(source);
    const auto new_source = pipeline.create_node<int_source>(-1, "new_source");
    pipeline.connect(new_source, c, 0);
    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"new_source"});
    polled.clear();

    CHECK(!pipeline.step());
    std::sort(polled.begin(), polled.end());
    CHECK(polled == std::vector<std::string>{"c", "new_source", "sink"});
    polled.clear();
}