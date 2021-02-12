//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_compiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class BoundFunctionExpression;
class ClientContext;
class PhysicalOperator;
class ExpressionState;
class DataChunk;
class Vector;

//! ExpressionCompiler is responsible for compiling complex expressions in a physical query plan
class ExpressionCompiler {
public:
	explicit ExpressionCompiler(ClientContext &context) : context(context) {
	}

	//! Traverse the physical query plan and compile complex expressions
	void CompileExpressions(PhysicalOperator &op);

	//! Get the time (in ms) it took to compile all complex expressions contained in the physical operator
	static uint64_t GetCompilationTime(PhysicalOperator &op);

private:
	//! Compile complex expressions of the child operators
	void CompileOperatorChildrenExpressions(PhysicalOperator &op);

	//! Compile complex expressions contained in the physical operator
	void CompileOperatorExpressions(PhysicalOperator &op);

	//! Compile the expression if it is complex
	void CompileExpression(Expression &expression);

	//! Compile the BoundFunctionExpression if it is complex.
	//! Return the children expressions of the BoundFunctionExpression that were not fused
	vector<unique_ptr<Expression>> &CompileExpression(BoundFunctionExpression &expr);

	ClientContext &context;
};

//! CompiledExpression is the result of compiling an expression
class CompiledExpression {
public:
	//! Compile the code into a shared library, load it and obtain a pointer to the symbol named function_name
	CompiledExpression(string code, const string &function_name, vector<unique_ptr<Expression>> input_expressions);

	CompiledExpression(CompiledExpression &&other) noexcept;
	CompiledExpression &operator=(CompiledExpression &&other) noexcept;
	CompiledExpression(const CompiledExpression &) = delete;
	CompiledExpression &operator=(const CompiledExpression &) = delete;

	//! Unload the shared library
	~CompiledExpression();

	//! Test if compilation and loading of the shared library is finished
	bool IsReady() const {
		return function != nullptr;
	}

	//! Should only be called when IsReady == true.
	//! Modifies expr and state s.t. the ExpressionExecutor executes the compiled code instead of the original
	//! scalar function referenced by expr
	void Upgrade(BoundFunctionExpression &expr, ExpressionState &state, DataChunk &arguments);

	vector<unique_ptr<Expression>> &GetInputExpressions() {
		return input_expressions;
	}

	uint64_t GetCompilationTime() const {
		return milliseconds;
	}

private:
	using function_t = void (*)(DataChunk &, ExpressionState &, Vector &);

	void *handle {nullptr};
	function_t function {nullptr};
	vector<unique_ptr<Expression>> input_expressions;
	uint64_t milliseconds {0};
};
} // namespace duckdb
